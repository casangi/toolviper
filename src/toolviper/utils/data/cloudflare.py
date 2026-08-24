import json
import os
import pathlib
import shutil
import time
import zipfile
from collections import defaultdict
from threading import Thread

import pandas as pd
import requests
from rich.console import Console
from rich.progress import Progress, TaskID

import toolviper.utils.console as console
import toolviper.utils.logger as logger
from toolviper.utils import parameter
from toolviper.utils.parameter import is_notebook

colorize = console.Colorize()

# Constants
PROGRESS_MAX_CHARACTERS = 28
BASE_URL = "https://downloadnrao.org"
METADATA_REL_PATH = ".cloudflare/file.download.json"
USER_AGENT = "Wget/1.16 (linux-gnu)"

# Download robustness. The read timeout only fires when *no* bytes arrive for
# that long; a connection that trickles a few bytes per second never trips it,
# so a separate minimum-average-rate check catches stalled-but-alive transfers
# (observed with the Cloudflare-fronted download server in CI). Stalled or
# failed attempts are retried from a fresh connection.
DOWNLOAD_CHUNK_SIZE = 64 * 1024  # bytes per iter_content chunk
DOWNLOAD_CONNECT_TIMEOUT = 30  # seconds to establish the connection
DOWNLOAD_READ_TIMEOUT = 120  # max seconds between bytes on the socket
DOWNLOAD_MAX_ATTEMPTS = 3
DOWNLOAD_RETRY_WAIT = 10  # seconds; scaled by the attempt number
DOWNLOAD_STALL_GRACE_PERIOD = 60  # seconds before the rate check applies
DOWNLOAD_STALL_MINIMUM_RATE = 64 * 1024  # bytes/s averaged over the attempt


class DownloadStalledError(RuntimeError):
    """A download attempt was aborted because it stalled or was truncated."""


def _get_metadata_path() -> pathlib.Path:
    """Get the absolute path to the local metadata file."""
    return pathlib.Path(__file__).parent.resolve().joinpath(METADATA_REL_PATH)


def version() -> None:
    """
    Print the version of the cloudflare manifest.
    """
    meta_data_path = _get_metadata_path()

    if not meta_data_path.parent.exists():
        logger.debug(f"Metadata path {meta_data_path.parent} doesn't exist... creating")
        meta_data_path.parent.mkdir(parents=True, exist_ok=True)

    _verify_metadata_file()

    try:
        with open(meta_data_path) as json_file:
            file_meta_data = json.load(json_file)
            logger.info(f"Manifest version: {file_meta_data.get('version', 'unknown')}")

    except (FileNotFoundError, json.JSONDecodeError) as e:
        logger.error(f"Failed to read metadata file: {e}")


@parameter.validate()
def download(
    file: str | list[str],
    folder: str = ".",
    overwrite: bool = False,
    decompress: bool = True,
) -> None:
    """
    Download tool for data stored externally.

    Parameters
    ----------
    file : str or list of str
        Filename(s) as stored on an external source.
    folder : str, optional
        Destination folder. Defaults to ".".
    overwrite : bool, optional
        Whether to overwrite existing files. Defaults to False.
    decompress : bool, optional
        Whether to unzip downloaded files. Defaults to True.
    """
    logger.info("Initializing download...")

    if isinstance(file, str):
        file = [file]

    # try:
    #    _print_file_queue(file)
    # except Exception as e:
    #    logger.warning(f"Problem printing file list: {e}")

    dest_path = pathlib.Path(folder).resolve()
    if not dest_path.exists():
        logger.info(f"Creating path: {colorize.blue(str(dest_path))}")
        dest_path.mkdir(parents=True, exist_ok=True)

    meta_data_path = _get_metadata_path()
    if not meta_data_path.exists():
        logger.warning(
            f"Metadata not found locally at {colorize.blue(str(meta_data_path))}"
        )
        update()

    try:
        with open(meta_data_path) as json_file:
            file_meta_data = json.load(json_file)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logger.error(f"Failed to load metadata: {e}")
        return

    tasks = []
    missing_files = []

    def name_format(string):
        return (
            f"{string[: (PROGRESS_MAX_CHARACTERS - 4)]} ..."
            if len(string) > PROGRESS_MAX_CHARACTERS
            else string
        )

    for f_name in file:
        full_file_path = dest_path.joinpath(f_name)

        if full_file_path.exists() and not overwrite:
            logger.info(f"File already exists: {full_file_path}")
            continue

        if f_name not in file_meta_data.get("metadata", {}):
            logger.error(f"Requested file not found in manifest: {f_name}")
            logger.error(
                f"Use {colorize.blue('toolviper.utils.data.update()')} for the most recent version of the manifest."
            )
            logger.info(
                f"Use {colorize.blue('toolviper.utils.data.list_files()')} for available files."
            )
            missing_files.append(f_name)
            continue

        meta = file_meta_data["metadata"][f_name]
        tasks.append(
            {
                "description": name_format(f_name),
                "metadata": meta,
                "folder": str(dest_path),
                "visible": True,
                "size": int(meta.get("size", 0)),
                "jupyter": is_notebook(),
            }
        )

    if not tasks:
        if missing_files:
            logger.error(f"Missing files: {missing_files}")
            raise RuntimeError(
                f"Files not found in the download manifest: {missing_files}"
            )

        return

    progress = Progress()
    if is_notebook():
        _ = Console(force_terminal=True, force_jupyter=False)
        _console = Console(force_jupyter=is_notebook())

        progress = Progress(console=_console)

    threads = []

    with progress:
        for task in tasks:
            task_id = progress.add_task(task["description"])
            thread = Thread(target=worker, args=(task_id, task, progress, decompress))
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

        progress.refresh()

    failed = [task["error"] for task in tasks if task.get("error")]

    if missing_files:
        logger.error(f"Could not download: {missing_files}")

    if failed or missing_files:
        raise RuntimeError(
            "Download failed: "
            + "; ".join(
                failed + [f"{f}: not in the download manifest" for f in missing_files]
            )
        )


def _download_attempt(
    task_id: TaskID, task: dict, url: str, fullname: pathlib.Path, progress: Progress
) -> None:
    """
    Run a single download attempt, writing the file to ``fullname``.

    Raises
    ------
    requests.RequestException
        On connection errors, HTTP error statuses, or read timeouts.
    DownloadStalledError
        If the average transfer rate drops below
        ``DOWNLOAD_STALL_MINIMUM_RATE`` after ``DOWNLOAD_STALL_GRACE_PERIOD``
        seconds, or the stream ends short of the advertised Content-Length.
    """
    response = requests.get(
        url,
        stream=True,
        headers={"user-agent": USER_AGENT},
        timeout=(DOWNLOAD_CONNECT_TIMEOUT, DOWNLOAD_READ_TIMEOUT),
    )
    try:
        response.raise_for_status()

        # The manifest size is only a display hint (it may be stale);
        # completeness is checked against the actual Content-Length header.
        content_length = int(response.headers.get("Content-Length", 0))
        total = content_length or task.get("size", 0)

        size = 0
        start = time.monotonic()
        if progress is not None:
            progress.update(task_id, completed=0, total=total, visible=task["visible"])

        with open(fullname, "wb") as fd:
            for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
                if not chunk:
                    continue

                size += fd.write(chunk)
                if progress is not None:
                    progress.update(
                        task_id, completed=size, total=total, visible=task["visible"]
                    )

                elapsed = time.monotonic() - start
                if (
                    elapsed > DOWNLOAD_STALL_GRACE_PERIOD
                    and size / elapsed < DOWNLOAD_STALL_MINIMUM_RATE
                ):
                    raise DownloadStalledError(
                        f"average rate {size / elapsed:.0f} B/s fell below "
                        f"{DOWNLOAD_STALL_MINIMUM_RATE} B/s after {elapsed:.0f} s "
                        f"({size}/{total or 'unknown'} bytes)"
                    )

        if content_length and size < content_length:
            raise DownloadStalledError(
                f"incomplete download: received {size} of {content_length} bytes"
            )
    finally:
        response.close()


def worker(
    task_id: TaskID, task: dict, progress: Progress = None, decompress: bool = True
) -> None:
    """
    Worker function to download a file in a thread.

    Retries stalled or failed attempts (up to ``DOWNLOAD_MAX_ATTEMPTS``) from a
    fresh connection. On final failure the error is recorded in
    ``task["error"]`` for the caller to report; nothing is raised because this
    runs in a worker thread.

    Parameters
    ----------
    progress : Progress
        Rich Progress instance.
    task_id : TaskID
        ID of the task in the progress bar.
    task : dict
        Task details including metadata and destination folder.
    decompress : bool, optional
        Whether to decompress the file after download. Defaults to True.
    """
    metadata = task["metadata"]
    filename = metadata["file"]
    path = metadata.get("path", "").strip("/")
    url = f"{BASE_URL}/{path}/{filename}" if path else f"{BASE_URL}/{filename}"

    dest_folder = pathlib.Path(task["folder"])
    fullname = dest_folder.joinpath(filename)

    last_error = None
    for attempt in range(1, DOWNLOAD_MAX_ATTEMPTS + 1):
        try:
            _download_attempt(task_id, task, url, fullname, progress)
            break

        except (requests.RequestException, DownloadStalledError) as e:
            last_error = e
            fullname.unlink(missing_ok=True)
            if attempt < DOWNLOAD_MAX_ATTEMPTS:
                wait = DOWNLOAD_RETRY_WAIT * attempt
                logger.warning(
                    f"Download attempt {attempt}/{DOWNLOAD_MAX_ATTEMPTS} failed "
                    f"for {filename}: {e}. Retrying in {wait} s..."
                )
                time.sleep(wait)

        except Exception as e:
            # Non-retryable (e.g. disk errors while writing).
            fullname.unlink(missing_ok=True)
            task["error"] = f"{filename}: {e}"
            logger.error(f"Error writing file {filename}: {e}")
            return
    else:
        task["error"] = f"{filename}: {last_error}"
        logger.error(
            f"Failed to download {filename} after {DOWNLOAD_MAX_ATTEMPTS} "
            f"attempts: {last_error}"
        )
        return

    if decompress and zipfile.is_zipfile(fullname):
        try:
            shutil.unpack_archive(filename=str(fullname), extract_dir=str(dest_folder))
            os.remove(fullname)
        except Exception as e:
            task["error"] = f"{filename}: failed to decompress: {e}"
            logger.error(f"Failed to decompress {filename}: {e}")


class ToolviperFiles:
    """
    Helper class for managing and displaying toolviper data manifests.
    """

    def __init__(self, manifest: str, dataframe: pd.DataFrame | None = None) -> None:
        self.manifest = manifest
        self.dataframe = dataframe
        self.notebook_mode = is_notebook()

        if self.notebook_mode:
            try:
                import itables

                itables.init_notebook_mode()

            except ImportError:
                logger.debug("itables not found, falling back to standard display.")

    def __call__(self) -> pd.DataFrame | None:
        if not self.notebook_mode:
            print(self.dataframe)
            return None

        return self.dataframe

    def print(self) -> pd.DataFrame | None:
        """
        Display the dataframe using appropriate formatting.
        """
        if not self.notebook_mode:
            try:
                import tabulate

                print(
                    tabulate.tabulate(
                        self.dataframe,
                        showindex=False,
                        headers=self.dataframe.columns,
                    )
                )
            except ImportError:
                print(self.dataframe)

            return None

        return self.dataframe

    @classmethod
    def from_manifest(cls, manifest: str) -> "ToolviperFiles":
        """
        Create a ToolviperFiles instance from a manifest file.
        """
        meta_data_path = pathlib.Path(manifest)

        try:
            with open(meta_data_path) as json_file:
                file_meta_data = json.load(json_file)

        except (FileNotFoundError, json.JSONDecodeError) as e:
            logger.error(f"Failed to load manifest {manifest}: {e}")

            return cls(manifest=manifest, dataframe=pd.DataFrame())

        metadata_dict = file_meta_data.get("metadata", {})
        data = defaultdict(list)

        for file_name, meta in metadata_dict.items():
            data["file"].append(file_name)

            for key, value in meta.items():
                if key == "file":
                    continue

                if key == "size":
                    try:
                        value = int(value)

                    except (ValueError, TypeError):
                        pass

                data[key].append(value)

        return cls(manifest=manifest, dataframe=pd.DataFrame(data))


def list_files(truncate: int | None = None) -> pd.DataFrame | None:
    """
    List all files available in the cloudflare manifest.

    Parameters
    ----------
    truncate : int, optional
        Maximum number of rows to display. Defaults to None.
    """
    pd.set_option("display.max_rows", truncate)
    pd.set_option("display.colheader_justify", "left")

    meta_data_path = _get_metadata_path()
    if not meta_data_path.exists():
        _verify_metadata_file()

    table = ToolviperFiles.from_manifest(str(meta_data_path))
    return table.print()


# This version of the function is now deprecated
def list_files_() -> None:
    """
    List all files in cloudflare
    """

    from rich.console import Console
    from rich.table import Table

    console = Console()

    table = Table(show_header=True, show_lines=True)

    meta_data_path = pathlib.Path(__file__).parent.joinpath(
        ".cloudflare/file.download.json"
    )

    # Verify that the download metadata exist and update if not.
    _verify_metadata_file()

    with open(meta_data_path) as json_file:
        file_meta_data = json.load(json_file)

        table.add_column("file", style="blue", no_wrap=False)
        table.add_column("dtype", style="green")
        table.add_column("telescope", style="green")
        table.add_column("size", style="green")
        table.add_column("mode", style="green")

        for filename in file_meta_data["metadata"].keys():
            values = [filename]

            for key, value in file_meta_data["metadata"][filename].items():
                if key in ["dtype", "telescope", "size", "mode"]:
                    values.append(value)

            table.add_row(*values)

    console.print(table)


def get_files() -> list[str]:
    """
    Get a list of all file names available in the cloudflare manifest.
    """
    meta_data_path = _get_metadata_path()
    _verify_metadata_file()

    try:
        with open(meta_data_path) as json_file:
            file_meta_data = json.load(json_file)
            return list(file_meta_data.get("metadata", {}).keys())

    except (FileNotFoundError, json.JSONDecodeError):
        return []


@parameter.validate()
def update(path: str | None = None) -> None:
    """
    Update the local cloudflare manifest by downloading the latest version.

    Parameters
    ----------
    path : str, optional
        Custom path to save the manifest to. Defaults to the internal .cloudflare directory.
    """
    if path is None:
        meta_data_dir = _get_metadata_path().parent
        meta_data_path = _get_metadata_path()

    else:
        meta_data_dir = pathlib.Path(path)
        meta_data_path = meta_data_dir.joinpath("file.download.json")

    if not meta_data_dir.exists():
        meta_data_dir.mkdir(parents=True, exist_ok=True)

    # Temporary metadata to kickstart the download of the actual manifest
    file_meta_data = {
        "file": "file.download.json",
        "path": "/",
        "dtype": "JSON",
        "telescope": "NA",
        "size": "23879",
        "mode": "NA",
    }

    task = {
        "description": "Updating manifest",
        "metadata": file_meta_data,
        "folder": str(meta_data_dir),
        "visible": True,
        "size": 23879,
    }

    logger.info("Updating file metadata information...")

    _console = Console(force_jupyter=is_notebook())

    with _console.status("[bold green]Working on download manifest update ..."):
        worker(task_id=0, task=task, progress=None, decompress=False)

    if task.get("error") or not meta_data_path.exists():
        logger.error("Unable to retrieve download metadata.")
        raise FileNotFoundError(f"Download metadata file not found at {meta_data_path}")


@parameter.validate()
def get_file_size(path: str) -> dict[str, int]:
    """
    Get file sizes in bytes for all files in a given path.

    Parameters
    ----------
    path : str
        The directory path to scan.

    Returns
    -------
    dict
        A dictionary mapping file names to their sizes in bytes.
    """
    path_obj = pathlib.Path(path).resolve()
    if not path_obj.exists() or not path_obj.is_dir():
        logger.error(f"Path not found or is not a directory: {path}")
        return {}

    file_size_dict = {}
    for item in path_obj.iterdir():
        if item.is_file():
            name = (
                item.name.split(".zip")[0] if item.name.endswith(".zip") else item.name
            )
            file_size_dict[name] = item.stat().st_size

    return file_size_dict


def _print_file_queue(files: list[str]) -> None:
    """
    Print a formatted list of files to be downloaded.
    """
    from rich import box
    from rich.console import Console
    from rich.table import Table

    assert isinstance(files, list), logger.error("files must be a list")

    console_ = Console()
    table = Table(show_header=True, box=box.SIMPLE)
    table.add_column("Download List", justify="left")

    for f_name in files:
        table.add_row(f"[magenta]{f_name}[/magenta]")

    console_.print(table)


def _verify_metadata_file() -> None:
    """
    Ensure the metadata file exists or trigger an update.
    """
    meta_data_path = _get_metadata_path()
    if not meta_data_path.exists():
        logger.warning(f"Metadata file {meta_data_path} missing. Updating...")
        update()

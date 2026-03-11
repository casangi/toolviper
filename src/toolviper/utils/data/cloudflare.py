import json
import os
import pathlib
import shutil
import zipfile
from threading import Thread
from typing import Any, Dict, List, Optional, Union

import requests
import pandas as pd

from rich.console import Console
from rich.progress import Progress, TaskID

import toolviper.utils.console as console
import toolviper.utils.logger as logger

from toolviper.utils import parameter
from toolviper.utils.parameter import is_notebook
from collections import defaultdict

colorize = console.Colorize()

# Constants
PROGRESS_MAX_CHARACTERS = 28
MINIMUM_CHUNK_SIZE = 1024
BASE_URL = "https://downloadnrao.org"
METADATA_REL_PATH = ".cloudflare/file.download.json"
USER_AGENT = "Wget/1.16 (linux-gnu)"


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
        with open(meta_data_path, "r") as json_file:
            file_meta_data = json.load(json_file)
            logger.info(f"Manifest version: {file_meta_data.get('version', 'unknown')}")

    except (FileNotFoundError, json.JSONDecodeError) as e:
        logger.error(f"Failed to read metadata file: {e}")


@parameter.validate()
def download(
    file: Union[str, List[str]],
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
        with open(meta_data_path, "r") as json_file:
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

    if missing_files:
        logger.error(f"Could not download: {missing_files}")


def worker(
    task_id: TaskID, task: dict, progress: Progress = None, decompress: bool = True
) -> None:
    """
    Worker function to download a file in a thread.

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

    try:
        response = requests.get(
            url, stream=True, headers={"user-agent": USER_AGENT}, timeout=30
        )
        response.raise_for_status()

    except Exception as e:
        logger.error(f"Failed to initiate download for {filename}: {e}")
        return

    total = int(response.headers.get("Content-Length", 0))
    if total == 0:
        total = task.get("size", 0)

    dest_folder = pathlib.Path(task["folder"])
    fullname = dest_folder.joinpath(filename)

    try:
        size = 0
        with open(fullname, "wb") as fd:
            for chunk in response.iter_content(chunk_size=MINIMUM_CHUNK_SIZE):
                if chunk:
                    size += fd.write(chunk)
                    if progress is not None:
                        progress.update(
                            task_id,
                            completed=size,
                            total=total,
                            visible=task["visible"],
                        )

    except Exception as e:
        logger.error(f"Error writing file {filename}: {e}")
        return

    if decompress and zipfile.is_zipfile(fullname):
        try:
            shutil.unpack_archive(filename=str(fullname), extract_dir=str(dest_folder))
            os.remove(fullname)
        except Exception as e:
            logger.error(f"Failed to decompress {filename}: {e}")


class ToolviperFiles:
    """
    Helper class for managing and displaying toolviper data manifests.
    """

    def __init__(self, manifest: str, dataframe: Optional[pd.DataFrame] = None) -> None:
        self.manifest = manifest
        self.dataframe = dataframe
        self.notebook_mode = is_notebook()

        if self.notebook_mode:
            try:
                import itables

                itables.init_notebook_mode()

            except ImportError:
                logger.debug("itables not found, falling back to standard display.")

    def __call__(self) -> Optional[pd.DataFrame]:
        if not self.notebook_mode:
            print(self.dataframe)
            return None

        return self.dataframe

    def print(self) -> Optional[pd.DataFrame]:
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
            with open(meta_data_path, "r") as json_file:
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


def list_files(truncate: Optional[int] = None) -> Optional[pd.DataFrame]:
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


def get_files() -> List[str]:
    """
    Get a list of all file names available in the cloudflare manifest.
    """
    meta_data_path = _get_metadata_path()
    _verify_metadata_file()

    try:
        with open(meta_data_path, "r") as json_file:
            file_meta_data = json.load(json_file)
            return list(file_meta_data.get("metadata", {}).keys())

    except (FileNotFoundError, json.JSONDecodeError):
        return []


@parameter.validate()
def update(path: Optional[str] = None) -> None:
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

    task_id = 0
    # with progress:
    _console = Console(force_jupyter=is_notebook())
    tasks = [f"\nManifest update "]

    with _console.status(
        "[bold green]Working on download manifest update ..."
    ) as status:
        while tasks:
            worker(task_id, task, progress=None, decompress=False)

            task = tasks.pop(0)

    if not meta_data_path.exists():
        logger.error("Unable to retrieve download metadata.")
        raise FileNotFoundError(f"Download metadata file not found at {meta_data_path}")


@parameter.validate()
def get_file_size(path: str) -> Dict[str, int]:
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


def _print_file_queue(files: List[str]) -> None:
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

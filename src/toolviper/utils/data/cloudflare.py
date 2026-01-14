import json
import os
import pathlib
import shutil
import zipfile
from threading import Thread
from typing import Any, Optional, Union

import requests
from rich.progress import Progress, TaskID

import toolviper
import toolviper.utils.console as console
import toolviper.utils.logger as logger
from toolviper.utils import parameter

from collections import defaultdict
from toolviper.utils.parameter import is_notebook
import pandas as pd

colorize = console.Colorize()

PROGRESS_MAX_CHARACTERS = 28
MINIMUM_CHUNK_SIZE = 1024


def version() -> None:
    # Load the file dropbox file meta data.
    meta_data_path = pathlib.Path(__file__).parent.joinpath(
        ".cloudflare/file.download.json"
    )

    if not meta_data_path.parent.exists():
        logger.debug("metadata path doesn't exist... creating")
        meta_data_path.parent.mkdir(parents=True)

    # Verify that the download metadata exists and updates if not.
    _verify_metadata_file()

    with open(meta_data_path) as json_file:
        file_meta_data = json.load(json_file)

        logger.info(f"{file_meta_data['version']}")


@parameter.validate()
def download(
    file: Union[str, list],
    folder: str = ".",
    overwrite: bool = False,
    decompress: bool = True,
) -> None:
    """
        Download tool for data stored externally.
    Parameters
    ----------
    file : str
        Filename as stored on an external source.
    folder : str
        Destination folder.
    overwrite : bool
        Should file be overwritten.
    decompress : bool
        Should file be unzipped.

    Returns
    -------
        No return
    """

    logger.info("Downloading from [cloudflare] ....")

    if not isinstance(file, list):
        file = [file]

    try:
        _print_file_queue(file)

    except Exception as e:
        logger.warning(f"There was a problem printing the file list... {e}")

    finally:
        if not pathlib.Path(folder).resolve().exists():
            toolviper.utils.logger.info(
                f"Creating path:{colorize.blue(str(pathlib.Path(folder).resolve()))}"
            )
            pathlib.Path(folder).resolve().mkdir()

    logger.debug(f"Initializing [cloudflare] downloader ...")

    meta_data_path = pathlib.Path(__file__).parent.joinpath(
        ".cloudflare/file.download.json"
    )

    tasks = []

    # Make a list of files that aren't available from cloudflare yet
    missing_files = []

    # Load the file dropbox file meta data.
    if not meta_data_path.exists():
        logger.warning(
            f"Couldn't find file metadata locally in {colorize.blue(str(meta_data_path))}"
        )

        toolviper.utils.data.update()

    with open(meta_data_path) as json_file:
        file_meta_data = json.load(json_file)

        # Build the task list
        for file_ in file:
            full_file_path = pathlib.Path(folder).joinpath(file_)

            if full_file_path.exists() and not overwrite:
                logger.info(f"File exists: {str(full_file_path)}")
                continue

            if file_ not in file_meta_data["metadata"].keys():
                logger.error(f"Requested file not found: {file_}")
                logger.info(
                    f"For a list of available files try using "
                    f"{colorize.blue('toolviper.utils.data.list_files()')}."
                )

                missing_files.append(file_)
                continue

            name_format = lambda string: (
                f"{string[: (PROGRESS_MAX_CHARACTERS - 4)]} ..."
                if len(string) > PROGRESS_MAX_CHARACTERS
                else string
            )

            tasks.append(
                {
                    "description": name_format(file_),
                    "metadata": file_meta_data["metadata"][file_],
                    "folder": folder,
                    "visible": True,
                    "size": int(file_meta_data["metadata"][file_]["size"]),
                }
            )

    threads = []
    progress = Progress()

    with progress:
        task_ids = [
            progress.add_task(task["description"]) for task in tasks if len(tasks) > 0
        ]

        for i, task in enumerate(tasks):
            thread = Thread(
                target=worker, args=(progress, task_ids[i], task, decompress)
            )
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

    if len(missing_files) > 0:
        logger.error(f"Missing files: {missing_files}")


def worker(progress: Progress, task_id: TaskID, task: dict, decompress=True) -> None:
    """Simulate work being done in a thread"""

    filename = task["metadata"]["file"]

    url = f"https://downloadnrao.org/{task['metadata']['path']}/{task['metadata']['file']}"

    r = requests.get(url, stream=True, headers={"user-agent": "Wget/1.16 (linux-gnu)"})
    total = int(r.headers.get("Content-Length", 0))

    if total == 0:
        total = task["size"]

    fullname = str(pathlib.Path(task["folder"]).joinpath(filename))

    size = 0

    with open(fullname, "wb") as fd:
        for chunk in r.iter_content(chunk_size=MINIMUM_CHUNK_SIZE):
            if chunk:
                size += fd.write(chunk)
                progress.update(
                    task_id, completed=size, total=total, visible=task["visible"]
                )

    # Verify checksum on file
    # toolviper.utils.verify(filename, task["folder"])

    if decompress:
        if zipfile.is_zipfile(fullname):
            shutil.unpack_archive(filename=fullname, extract_dir=task["folder"])

            # Let's clean up after ourselves
            os.remove(fullname)


class ToolviperFiles:
    def __init__(self, manifest, dataframe=None):

        self.manifest = manifest
        self.dataframe = dataframe
        self.notebook_mode = False

        if is_notebook():
            import itables

            self.notebook_mode = True

            itables.init_notebook_mode()

    def __call__(self):
        if not self.notebook_mode:
            return print(self.dataframe)

        else:
            return self.dataframe

    def print(self)->Union[None, pd.DataFrame]:
        if not self.notebook_mode:
            import tabulate
            print(tabulate.tabulate(self.dataframe, showindex=False, headers=self.dataframe.columns))
            return None

        return self.dataframe


    @classmethod
    def from_manifest(cls, manifest: str):
        meta_data_path = pathlib.Path(manifest)

        # Verify that the download metadata exist and update if not.
        # _verify_metadata_file()

        with open(meta_data_path) as json_file:

            file_meta_data = json.load(json_file)

            files = file_meta_data["metadata"].keys()

            data = defaultdict(list)
            data["file"] = list(files)

            for file_, metadata_ in file_meta_data["metadata"].items():
                for key_, value_ in metadata_.items():
                    if key_ == "file":
                        continue

                    # I think we could do this with a JSON ENCODER
                    # but this is easier since the file is small
                    # and everything is a string already

                    if value_ == "size":
                        value_ = int(value_)


                    data[key_].append(value_)

            return cls(manifest=manifest, dataframe=pd.DataFrame(data))



def list_files(truncate=None) -> pd.DataFrame:

    pd.set_option('display.max_rows', truncate)
    pd.set_option('display.colheader_justify', 'left')

    meta_data_path = pathlib.Path(__file__).parent.joinpath(
        ".cloudflare/file.download.json"
    )

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


def get_files() -> list[Any]:
    """
    Get all files available in cloudflare manifest. This is retrieved from the local cloudflare
    metadata file.

    """
    meta_data_path = pathlib.Path(__file__).parent.joinpath(
        ".cloudflare/file.download.json"
    )

    # Verify that the download metadata exists and updates if not.
    _verify_metadata_file()

    with open(meta_data_path) as json_file:
        file_meta_data = json.load(json_file)

        return list(file_meta_data["metadata"].keys())


@parameter.validate()
def update(path: Union[str, None] = None) -> None:
    """
    Update cloudflare manifest.

    Parameters
    ----------
    path : str
        In the case that you want an updated copy of the manifest for modification, this is the path to save it to.
    """

    if path is None:
        meta_data_path = pathlib.Path(__file__).parent.joinpath(".cloudflare")

    else:
        # I know this is an unnecessary copy but I don't want a big erbose path name in the inpute variables.
        meta_data_path = pathlib.Path(path)

    if not meta_data_path.exists():
        _make_dir(str(pathlib.Path(__file__).parent), ".cloudflare")

    file_meta_data = {
        "file": "file.download.json",
        "path": "/",
        "dtype": "JSON",
        "telescope": "NA",
        "size": "12484",
        "mode": "NA",
    }

    tasks = {
        "description": "file.download.json",
        "metadata": file_meta_data,
        "folder": meta_data_path,
        "visible": False,
        "size": 12484,
    }

    logger.info("Updating file metadata information ... ")

    progress = Progress()
    task_id = progress.add_task(tasks["description"])

    with progress:
        worker(progress, task_id, tasks)

    if not meta_data_path.exists():
        logger.error("Unable to retrieve download metadata.")
        raise FileNotFoundError(
            "Download metadata file does not exist at the expected path."
        )


@parameter.validate()
def get_file_size(path: str) -> Optional[dict]:
    """
    Get list file sizes in bytes for a given path. Only works for files; isn't recursive.
    """
    if not pathlib.Path(path).resolve().exists():
        logger.error(f"Path not found...: {path}")

        return None

    file_size_dict = {}

    for item in pathlib.Path(path).resolve().iterdir():
        if pathlib.Path(item).resolve().is_file():
            if item.name.endswith(".zip"):
                item_ = item.name.split(".zip")[0]

            else:
                item_ = item.name

            file_size_dict[item_] = os.path.getsize(pathlib.Path(item))

    return file_size_dict


def _print_file_queue(files: list) -> None:
    from rich import box
    from rich.console import Console
    from rich.table import Table

    assert type(files) == list

    console = Console()
    table = Table(show_header=True, box=box.SIMPLE)

    table.add_column("Download List", justify="left")

    for file in files:
        table.add_row(f"[magenta]{file}[/magenta]")

    console.print(table)


def _make_dir(path, folder):
    p = pathlib.Path(path).joinpath(folder)
    try:
        p.mkdir()
        logger.info(
            f"Creating path:{colorize.blue(str(pathlib.Path(folder).resolve()))}"
        )

    except FileExistsError:
        logger.warning(f"File exists: {colorize.blue(str(p.resolve()))}")

    except FileNotFoundError:
        logger.warning(
            f"One fo the parent directories cannot be found: {colorize.blue(str(p.resolve()))}"
        )


def _verify_metadata_file():
    meta_data_path = pathlib.Path(__file__).parent.joinpath(
        ".cloudflare/file.download.json"
    )

    if not meta_data_path.exists():
        logger.warning(f"Couldn't find {colorize.blue(str(meta_data_path))}.")
        update()

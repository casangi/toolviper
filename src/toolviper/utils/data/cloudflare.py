import os
import sys
import shutil
import requests
import zipfile
import json
import psutil
import pathlib
import toolviper

from threading import Thread
from rich.progress import Progress

import toolviper.utils.logger as logger

from typing import NoReturn, Union
import toolviper.utils.console as console

colorize = console.Colorize()


def version():
    # Load the file dropbox file meta data.
    meta_data_path = pathlib.Path(__file__).parent.joinpath(
        ".cloudflare/file.download.json"
    )

    # Verify that the download metadata exists and updates if not.
    _verify_metadata_file()

    with open(meta_data_path) as json_file:
        file_meta_data = json.load(json_file)

        logger.info(f'{file_meta_data["version"]}')


def download(
    file: Union[str, list],
    folder: str = ".",
    threaded: bool = True,
    n_threads: Union[None, int] = None,
    overwrite: bool = False,
    decompress: bool = False,
) -> NoReturn:
    """
        Download tool for data stored externally.
    Parameters
    ----------
    file : str
        Filename as stored on an external source.
    folder : str
        Destination folder.
    threaded : bool
        File metadata download type.
    n_threads : int
        Number of threads to use.
    overwrite : bool
        Should file be overwritten.
    decompress : bool
        Should file be unzipped.

    Returns
    -------
        No return
    """

    logger.info(f"Downloading [cloudflare]: {file}")

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

    # Load the file dropbox file meta data.
    if meta_data_path.exists():
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

                    continue

                tasks.append(
                    {
                        "description": file_,
                        "metadata": file_meta_data["metadata"][file_],
                        "folder": folder,
                        "visible": True,
                    }
                )

    else:
        logger.warning(
            f"Couldn't find file metadata locally in {colorize.blue(str(meta_data_path))}"
        )

        toolviper.utils.data.update()
        return None

    threads = []
    progress = Progress()

    with progress:
        task_ids = [
            progress.add_task(task["description"]) for task in tasks if len(tasks) > 0
        ]

        for i, task in enumerate(tasks):
            thread = Thread(target=worker, args=(progress, task_ids[i], task))
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()


def worker(progress, task_id, task):
    """Simulate work being done in a thread"""

    fullname = task["metadata"]["file"]

    url = (
        f"http://downloadnrao.org/{task["metadata"]["path"]}/{task["metadata"]["file"]}"
    )

    r = requests.get(url, stream=True, headers={"user-agent": "Wget/1.16 (linux-gnu)"})

    fullname = str(pathlib.Path(task["folder"]).joinpath(fullname))

    size = 0
    with open(fullname, "wb") as fd:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:
                size += fd.write(chunk)
                progress.update(task_id, completed=size, visible=task["visible"])

    if zipfile.is_zipfile(fullname):
        shutil.unpack_archive(filename=fullname, extract_dir=task["folder"])

        # Let's clean up after ourselves
        os.remove(fullname)


def list_files():
    from rich.table import Table
    from rich.console import Console

    console = Console()

    table = Table(show_header=True, show_lines=True)

    meta_data_path = pathlib.Path(__file__).parent.joinpath(
        ".cloudflare/file.download.json"
    )

    # Verify that the download metadata exist and update if not.
    _verify_metadata_file()

    with open(meta_data_path) as json_file:
        file_meta_data = json.load(json_file)

        table.add_column("file", style="blue")
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


def get_files():
    meta_data_path = pathlib.Path(__file__).parent.joinpath(
        ".cloudflare/file.download.json"
    )

    # Verify that the download metadata exists and updates if not.
    _verify_metadata_file()

    with open(meta_data_path) as json_file:
        file_meta_data = json.load(json_file)

        return list(file_meta_data["metadata"].keys())


def update():
    meta_data_path = pathlib.Path(__file__).parent.joinpath(".cloudflare")

    _makedir(str(pathlib.Path(__file__).parent), ".cloudflare")

    file_meta_data = {
        "file": "file.download.json",
        "path": "/",
        "dtype": "JSON",
        "telescope": "NA",
        "size": "13575",
        "mode": "NA",
    }

    tasks = {
        "description": "file.download.json",
        "metadata": file_meta_data,
        "folder": meta_data_path,
        "visible": False,
    }

    logger.info("Updating file metadata information ... ")

    progress = Progress()
    task_id = progress.add_task(tasks["description"])

    with progress:
        worker(progress, task_id, tasks)

    # assert meta_data_path.exists() is True, logger.error("Unable to retrieve download metadata.")


def _print_file_queue(files: list) -> NoReturn:
    from rich.table import Table
    from rich.console import Console
    from rich import box

    console = Console()
    table = Table(show_header=True, box=box.SIMPLE)

    table.add_column("Download List", justify="left")

    for file in files:
        table.add_row(f"[magenta]{file}[/magenta]")

    console.print(table)


def _makedir(path, folder):
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
        logger.warning(f"Couldn't find {colorize.blue(meta_data_path)}.")
        update()

import os
import shutil
import requests
import zipfile
import json
import psutil
import pathlib
import toolviper
import concurrent.futures

import toolviper.utils.logger as logger

from typing import NoReturn, Union
import toolviper.utils.console as console

colorize = console.Colorize()


def version():
    # Load the file dropbox file meta data.
    meta_data_path = pathlib.Path(__file__).parent.joinpath(
        ".dropbox/file.download.json"
    )

    # Verify that the download metadata exist and update if not.
    _verify_metadata_file()

    with open(meta_data_path) as json_file:
        file_meta_data = json.load(json_file)

        logger.info(f'{file_meta_data["version"]}')


def download(
    file: Union[str, list],
    folder: str = ".",
    threaded: bool = True,
    n_threads: Union[None, int] = None,
) -> NoReturn:
    """
        Download tool for data stored externally.
    Parameters
    ----------
    file : str
        Filename as stored on external source.
    folder : str
        Destination folder.
    threaded : bool
        File metadata download type.
    n_threads : int
        Number of threads to use.

    Returns
    -------
        No return
    """

    toolviper.utils.data.update()

    if not pathlib.Path(folder).resolve().exists():
        toolviper.utils.logger.info(
            f"Creating path:{colorize.blue(str(pathlib.Path(folder).resolve()))}"
        )
        pathlib.Path(folder).resolve().mkdir()

    if threaded is False:
        _download(file=file, folder=folder)

    else:

        if not isinstance(file, list):
            file = [file]

        if n_threads is None:
            n_threads = _get_usable_threads(len(file))

        logger.debug(f"Initializing downloader with {n_threads} threads.")

        _print_file_queue(file)

        with concurrent.futures.ThreadPoolExecutor(max_workers=n_threads) as executor:
            for _file in file:
                executor.submit(_download, _file, folder)


def list_files():
    from rich.table import Table
    from rich.console import Console

    console = Console()

    table = Table(show_header=True, show_lines=True)

    meta_data_path = pathlib.Path(__file__).parent.joinpath(
        ".dropbox/file.download.json"
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
        ".dropbox/file.download.json"
    )

    # Verify that the download metadata exist and update if not.
    _verify_metadata_file()

    with open(meta_data_path) as json_file:
        file_meta_data = json.load(json_file)

        return list(file_meta_data["metadata"].keys())


def update():
    meta_data_path = pathlib.Path(__file__).parent.joinpath(".dropbox")

    _makedir(str(pathlib.Path(__file__).parent), ".dropbox")

    file_meta_data = {
        "metadata": {
            "file.download.json": {
                "file": "file.download.json",
                "id": "1m53led1mchpdc4m3pv37",
                "rlkey": "enkp8m1hv437nu6p020owflrt&st=11psoc6n",
            }
        }
    }

    logger.info("Updating file metadata information ... ")

    # Download metadata without visual indicator bar.
    _get_from_dropbox(
        file="file.download.json",
        folder=str(meta_data_path),
        file_meta_data=file_meta_data,
        bar=False,
    )

    # assert meta_data_path.exists() is True, logger.error("Unable to retrieve download metadata.")


def _get_usable_threads(n_files: int) -> int:
    # Always leave a single thread resource
    available_threads = psutil.cpu_count(logical=True) - 1

    if available_threads >= n_files:
        return n_files

    return int(available_threads)


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


def _is_notebook() -> bool:
    """
        Determines if code is running in  jupyter notebook.
    Returns
    -------
        bool

    """
    try:
        from IPython import get_ipython

        shell = get_ipython().__class__.__name__
        if shell == "ZMQInteractiveShell":
            return True
        else:
            raise ImportError

    except ImportError:
        return False


def _get_from_dropbox(file: str, folder: str, file_meta_data: dict, bar=True) -> None:
    fullname = file_meta_data["metadata"][file]["file"]
    id = file_meta_data["metadata"][file]["id"]
    rlkey = file_meta_data["metadata"][file]["rlkey"]

    url = "https://www.dropbox.com/scl/fi/{id}/{file}?rlkey={rlkey}".format(
        id=id, file=fullname, rlkey=rlkey
    )

    r = requests.get(url, stream=True, headers={"user-agent": "Wget/1.16 (linux-gnu)"})
    total = int(r.headers.get("content-length", 0))

    fullname = str(pathlib.Path(folder).joinpath(fullname))

    if _is_notebook():
        from tqdm.notebook import tqdm
    else:
        from tqdm import tqdm

    print(" ", end="", flush=True)

    # Is there a cleaner way to do this?
    if bar:
        with (
            open(fullname, "wb") as fd,
            tqdm(
                desc=fullname,
                total=total,
                unit="iB",
                unit_scale=True,
                unit_divisor=1024,
            ) as bar,
        ):
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    size = fd.write(chunk)
                    bar.update(size)

    else:
        with open(fullname, "wb") as fd:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    size = fd.write(chunk)


def _download(file: str, folder: str = ".") -> NoReturn:
    """
        Download tool for data stored on dropbox.
    Parameters
    ----------
    file : str
        Filename as stored on dropbox.
    folder : str
        Destination folder.

    Returns
    -------
        No return
    """

    # Load the file dropbox file meta data.
    meta_data_path = pathlib.Path(__file__).parent.joinpath(
        ".dropbox/file.download.json"
    )

    if meta_data_path.exists():
        with open(meta_data_path) as json_file:
            file_meta_data = json.load(json_file)

        full_file_path = pathlib.Path(folder).joinpath(file)

        if full_file_path.exists():
            logger.info("File exists: {file}".format(file=str(full_file_path)))
            return

        if file not in file_meta_data["metadata"].keys():
            logger.error(f"Requested file not found: {file}")
            logger.info(
                f"For a list of available files try using "
                f"{colorize.blue('toolviper.utils.data.list_files()')}."
            )

            return

    else:

        logger.wanring(
            f"Couldn't find file metadata locally in {colorize.blue(str(meta_data_path))}, trying to retrieve ..."
        )

        toolviper.utils.data.update()

        return

    fullname = file_meta_data["metadata"][file]["file"]
    fullname = str(pathlib.Path(folder).joinpath(fullname))

    _get_from_dropbox(file, folder, file_meta_data)

    if zipfile.is_zipfile(fullname):
        shutil.unpack_archive(filename=fullname, extract_dir=folder)

        # Let's clean up after ourselves
        os.remove(fullname)


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
        ".dropbox/file.download.json"
    )

    if not meta_data_path.exists():
        logger.warning(f"Couldn't find {colorize.blue(meta_data_path)}.")
        update()

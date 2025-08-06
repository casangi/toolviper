import json
import pathlib
import hashlib
import inspect

import toolviper.utils.logger as logger

from typing import Union, NoReturn


def open_json(file: str) -> Union[dict, NoReturn]:
    if not pathlib.Path(file).exists():
        logger.error(f"{file} doesn't exist ... exiting.")
        raise FileNotFoundError

    with open(file, "rb") as file_:
        json_file = json.load(file_)

    return json_file


def calculate_checksum(file) -> str:
    with open(file, "rb") as file_:
        digest = hashlib.file_digest(file_, "sha256")

    return digest.hexdigest()


def iter_files_(path):
    if not pathlib.Path(path).resolve().exists():
        logger.error(f"Path not found...: {path}")

        raise FileNotFoundError

    for item in pathlib.Path(path).resolve().iterdir():
        yield item.name


def update_hash(file, folder):
    json_file = open_json(file)

    for filename in iter_files_(folder):
        try:
            full_filename = pathlib.Path(folder).joinpath(filename)

            if full_filename.is_dir():
                logger.warning(
                    f"{filename} is a folder, run your favorite compression algorithm to calculate the checksum"
                )
                continue

            if str(full_filename).endswith(".zip"):
                filename = str(filename).split(".zip")[0]

            json_file["metadata"][filename]["hash"] = calculate_checksum(full_filename)

        except KeyError:
            logger.error(f"{filename} not found in metadata ...")
            pass

    with open(file, "w") as file_:
        json.dump(json_file, file_)


def verify(filename, folder):
    import toolviper

    fullname = str(pathlib.Path(folder).joinpath(filename))

    if not pathlib.Path(folder).exists():
        raise FileNotFoundError

    base_address = pathlib.Path(toolviper.__file__).parent
    metadata_address = base_address.joinpath(
        "utils/data/.cloudflare/file.download.json"
    )

    if metadata_address.exists():
        if filename.endswith(".zip"):
            filename = filename.split(".zip")[0]

            metadata = open_json(str(metadata_address))

            # Verify the downloaded file
            if (
                not metadata["metadata"][filename]["hash"]
                == toolviper.utils.tools.calculate_checksum(fullname)[:2]
            ):
                line_number = inspect.currentframe().f_back.f_lineno
                raise ChecksumError(
                    message="Checksum verification failed.",
                    filename=filename,
                    folder=folder,
                    line_number=line_number,
                )

    else:
        logger.error(f"{metadata_address} doesn't exist ... exiting.")
        raise FileNotFoundError


class ChecksumError(Exception):
    def __init__(self, message, filename, folder, line_number):
        self.message = message
        self.filename = filename
        self.folder = folder
        self.line_number = line_number

        super().__init__(self.message)

    def __str__(self):
        file = pathlib.Path(self.folder).joinpath(self.filename)
        return (
            f"[{self.line_number}]: There was an error verifying the checksum of {file}"
        )

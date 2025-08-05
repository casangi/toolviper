import json
import pathlib
import hashlib

import toolviper.utils.logger as logger

from typing import Union, NoReturn


def open_json_(file: str) -> Union[dict, NoReturn]:
    if not pathlib.Path(file).exists():
        logger.error(f"{file} doesn't exist ... exiting.")
        raise FileNotFoundError

    with open(file, "rb") as file_:
        json_file = json.load(file_)

    return json_file

def calculate_checksum(file)->str:
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
    json_file = open_json_(file)

    for filename in iter_files_(folder):
        try:
            full_filename = pathlib.Path(folder).joinpath(filename)

            if full_filename.is_dir():
                logger.warning(
                    f"{filename} is a folder, run your favorite compression algorithm to calculate the checksum")
                continue

            if str(full_filename).endswith(".zip"):
                filename = str(filename).split(".zip")[0]

            json_file["metadata"][filename]["hash"] = calculate_checksum(full_filename)

        except KeyError:
            logger.error(f"{filename} not found in metadata ...")
            pass

    with open(file, "w") as file_:
        json.dump(json_file, file_)
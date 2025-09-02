import json
import pathlib
import hashlib
import inspect

import toolviper.utils.logger as logger

from toolviper.utils import parameter

from typing import Union, NoReturn


def open_json(file: str) -> Union[dict, NoReturn]:
    if not pathlib.Path(file).exists():
        logger.error(f"{file} doesn't exist ... exiting.")
        raise FileNotFoundError

    with open(file, "rb") as file_:
        json_file = json.load(file_)

    return json_file


def calculate_checksum(file: str) -> str:
    with open(file, "rb") as file_:
        digest = hashlib.file_digest(file_, "sha256")

    return digest.hexdigest()


def iter_files_(path):
    if not pathlib.Path(path).resolve().exists():
        logger.error(f"Path not found ... : {path}")

        raise FileNotFoundError

    for item in pathlib.Path(path).resolve().iterdir():
        yield item.name


def update_hash(file: str, folder: str):
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


def verify(filename: str, folder: str):
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
            if not metadata["metadata"][filename][
                "hash"
            ] == toolviper.utils.tools.calculate_checksum(fullname):

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


@parameter.validate()
def add_entry(
    file: str,
    path: str,
    dtype: str,
    telescope: str,
    mode: str,
    versioning: str = "patch",
) -> Union[None, dict]:
    """
        Build new file.download.json with added metadata.

    Parameters
    ----------
    file : str
        Filename of file to upload.
    path : str
        Cloudflare path.
    dtype : bool
        File type of file to upload.
    telescope : bool
        Telescope data was taken with.
    mode : bool
        Telescope data mode.

    versioning : str
        Type of version update: major, minor, patch

    Returns
    -------
        No return
    """
    import toolviper

    try:
        metadata = {}

        manifest_path = pathlib.Path(toolviper.__path__[0]).joinpath(
            "utils/data/.cloudflare/file.download.json"
        )

        if not manifest_path.exists():
            logger.error(f"Couldn't find download manifest at: {manifest_path}")
            return None

        json_file = toolviper.utils.tools.open_json(str(manifest_path))

        json_file["version"] = update_version(versioning=versioning)

        filename = pathlib.Path(file)
        if filename.is_dir():
            logger.warning(
                f"{filename.name} is a folder, run your favorite compression algorithm to calculate the checksum"
            )

            return None

        file_key = str(filename.name)
        if str(filename).endswith(".zip"):
            file_key = str(filename.name).split(".zip")[0]

        size = toolviper.utils.data.get_file_size(path=str(filename.parent))[file_key]

        metadata = {
            "file": filename.name,
            "path": path,
            "dtype": dtype,
            "telescope": telescope,
            "size": str(size),
            "mode": mode,
            "hash": toolviper.utils.tools.calculate_checksum(str(filename)),
        }

        json_file["metadata"][file_key] = metadata

    except KeyError:
        logger.error(f"{file_key} not found in metadata ...")
        return None

    with open("file.download.json", "w") as file_:
        json.dump(json_file, file_)

    return json_file


def update_version(versioning="patch"):
    import toolviper

    manifest_path = pathlib.Path(toolviper.__path__[0]).joinpath(
        "utils/data/.cloudflare/file.download.json"
    )

    if not manifest_path.exists():
        logger.error(f"Couldn't find download manifest at: {manifest_path}")
        return None

    json_file = toolviper.utils.tools.open_json(str(manifest_path))

    version_number = json_file["version"]

    major, minor, patch = version_number.split(".")

    major = major.lstrip("v")

    match versioning:
        case "major":
            major = int(major) + 1
            major = str(major)

        case "minor":
            minor = int(minor) + 1
            minor = str(minor)

        case "patch":
            patch = int(patch) + 1
            patch = str(patch)

        case _:
            logger.error(f"Unknown option: {versioning}")
            return None

    version_number = f"v{major}.{minor}.{patch}"

    return version_number


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

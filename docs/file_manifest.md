## Updating the File Manifest

#### Adding a new file to the cloudflare downloader has two steps:

- Copy the file to the proper cloudflare R2 bucket. Make sure to take note of the cloudflare R2 bucket file location.
- A function to build a new `file.download.json` with an added metadata entry can be done using the `toolviper.utils.tools.add_entry(...)` function. __It is important that the file be in zipfile form; the code will fail otherwise__.

```
def add_entry(
    entries: Union[list, dict],
    manifest: Union[str, pathlib.Path, None] = None,
    versioning: str = "patch",
) -> Union[dict, None]:
    """
        Build new file.download.json with added metadata.

    Parameters
    ----------

    entries : list
        Dictionary of metadata info that are needed to build the new entry.

    manifest : str
        Points to the manifest you want to modify.

    versioning : str
        Type of version update: major, minor, patch

    Returns
    -------
    dict, None
    """
  ```
  
 The new file will be saved in the local directory and can then be uploaded to cloudflare in the base directory of the public-data bucket. See the example notebook (file-manifest-update.ipynb) for usage with example files.

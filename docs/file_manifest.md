## Updating the File Manifest

#### Adding a new file to the cloudflare downloader has two steps:

- Copy the file to the proper cloudflare R2 bucket. Make sure to take note of the cloudflare R2 bucket file location.
- A function to build a new `file.download.json` with an added metadata entry can be done using the `toolviper.utils.tools.add_entry(...)` function.

```
def add_entry(file: str, path: str, dtype: str, telescope: str, mode: str) -> Union[None, dict]:  
    """  
 Build new file.download.json with added metadata.  
 
 Parameters 
 ---------- 
 file : str 
	 Filename of file to upload. 
	 
 path : str 
	 Cloudflare path, ie. radps/image

dtype : bool File type of file to upload. 
	 
 telescope : bool 
	 Telescope data was taken with. 
	 
 mode : bool 
	 Telescope data mode.  
 
 Returns 
 ------- 
 None || dict
 """
 ```
 The new file will be saved in the local directory and can then be uploaded to cloudflare in the base directory of the public-data bucket.
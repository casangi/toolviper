import pathlib

import toolviper
import toolviper.utils.logger as logger


class TestToolViperDownload:
    @classmethod
    def setup_class(cls):
        """setup any state specific to the execution of the given test class
        such as fetching test data"""
        pass

    @classmethod
    def teardown_class(cls):
        """teardown any state that was previously setup with a call to setup_class
        such as deleting test data"""
        # cls.client.shutdown()
        pass

    def setup_method(self):
        """setup any state specific to all methods of the given class"""
        toolviper.utils.data.update()

    def teardown_method(self):
        """teardown any state that was previously setup for all methods of the given class"""
        import shutil

        path = pathlib.Path.cwd().joinpath("data")
        shutil.rmtree(str(path), ignore_errors=True)

    def test_download_verification(self):

        path = pathlib.Path.cwd().joinpath("data")
        path.mkdir(parents=True, exist_ok=True)

        # Only test the first two for now.
        files = [
            "ea25_cal_small_after_fixed.split.ms",
            "ea25_cal_small_before_fixed.split.ms",
        ]
        toolviper.utils.data.download(file=files, folder=str(path))

        # Get metadate json
        base_address = pathlib.Path(toolviper.__file__).parent
        metadata_address = base_address.joinpath(
            "utils/data/.cloudflare/file.download.json"
        )

        metadata = toolviper.utils.tools.open_json(str(metadata_address))

        for file in files:
            toolviper.utils.tools.verify(filename=file, folder=path)

    def test_download_folder(self):

        path = pathlib.Path.cwd().joinpath("data")
        path.mkdir(parents=True, exist_ok=True)

        file = toolviper.utils.data.get_files()[0]
        toolviper.utils.data.download(file=file, folder=str(path))

        assert path.joinpath(file).exists() == True

    def test_download_overwrite(self):

        path = pathlib.Path.cwd().joinpath("data")
        path.mkdir(parents=True, exist_ok=True)

        file = toolviper.utils.data.get_files()[0]
        file_path = path.joinpath(file)

        # Download first time
        toolviper.utils.data.download(file=file, folder=str(path), overwrite=True)
        original_file_timestamp = file_path.stat().st_mtime

        file_path.touch(exist_ok=True)
        # Download second time
        toolviper.utils.data.download(file=file, folder=str(path), overwrite=True)
        final_file_timestamp = file_path.stat().st_mtime

        assert original_file_timestamp != final_file_timestamp

    def test_download_decompress(self):

        path = pathlib.Path.cwd().joinpath("data")
        path.mkdir(parents=True, exist_ok=True)

        file = "ea25_cal_small_after_fixed.split.ms"

        toolviper.utils.data.download(
            file=file, folder=str(path), decompress=False, overwrite=True
        )

        # Check that the zip file exists
        assert path.joinpath(f"{file}.zip").exists() == True

        # Check that folder does not exist
        assert path.joinpath(file).exists() == False

        # Check that file isn't a folder
        assert path.joinpath(file).is_dir() == False

    def test_update(self):

        meta_data_path = pathlib.Path(toolviper.__file__).parent.joinpath(
            "utils/data/.cloudflare/file.download.json"
        )

        original_file_timestamp = meta_data_path.stat().st_mtime

        meta_data_path.touch(exist_ok=True)

        toolviper.utils.data.update()
        final_file_timestamp = meta_data_path.stat().st_mtime

        # Check that the file was updated
        assert original_file_timestamp != final_file_timestamp

    def test_get_file_size(self):

        path = pathlib.Path.cwd().joinpath("data")
        path.mkdir(parents=True, exist_ok=True)

        meta_data_path = pathlib.Path(toolviper.__file__).parent.joinpath(
            "utils/data/.cloudflare/file.download.json"
        )

        meta_data_file = toolviper.utils.tools.open_json(str(meta_data_path))

        file = toolviper.utils.data.get_files()[0]

        toolviper.utils.data.download(
            file=file, folder=str(path), decompress=False, overwrite=True
        )

        file_size_dict = toolviper.utils.data.get_file_size(path=str(path))

        assert str(file_size_dict[file]) == meta_data_file["metadata"][file]["size"]

    def test_private_print_file_queue(self):
        from toolviper.utils.data.cloudflare import _print_file_queue

        try:
            _print_file_queue(files="")

        except AssertionError as e:
            logger.info("Failure test passed!")
            return None

        # If error isn't as expected, fail the test.
        raise AssertionError()

    def test_private_make_dir(self):
        from toolviper.utils.data.cloudflare import _make_dir

        _make_dir(path=str(pathlib.Path.cwd()), folder="data")

        if not pathlib.Path.cwd().joinpath("data").exists():
            raise FileNotFoundError("data")

import pathlib




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
        pass

    def teardown_method(self):
        """teardown any state that was previously setup for all methods of the given class"""
        pass

    def test_download_fallback(self):
        import toolviper

        # Make data-path
        path = pathlib.Path.cwd().joinpath("data")
        path.mkdir(parents=True, exist_ok=True)

        toolviper.utils.data.download(file="dropbox", folder=str(path))

        if not path.joinpath("dropbox.txt").exists():
            raise FileNotFoundError("dropbox.txt")
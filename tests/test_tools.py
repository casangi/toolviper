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
        pass

    def teardown_method(self):
        """teardown any state that was previously setup for all methods of the given class"""
        pass

    def test_open_json(self):
        from toolviper.utils.tools import open_json

        try:
            open_json("tests/data/test.json")

        except FileNotFoundError:
            logger.info(f"Function open_json(...) working as expected.")
            return None

        raise AssertionError

    def test_private_iter_files(self):
        from toolviper.utils.tools import iter_files_

        try:
            for _ in iter_files_("tests/data/"):
                pass

        except FileNotFoundError:
            logger.info(f"Function iter_files_(...) working as expected.")
            return None

        raise AssertionError

    def test_verify(self):
        from toolviper.utils.tools import verify

        try:
            # Test files that doesn't exist
            verify(filename="test.json", folder="tests/data")

        except FileNotFoundError:
            logger.info(f"Function verify(...) working as expected.")
            return None

        raise AssertionError

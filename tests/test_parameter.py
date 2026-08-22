import json
import os
from unittest.mock import MagicMock, patch

import pytest

from toolviper.utils.parameter import (
    get_path,
    is_notebook,
    set_config_directory,
    validate,
    verify,
)


def test_is_notebook():
    # Should be False in normal python environment
    assert is_notebook() is False


def test_get_path_standard(monkeypatch):
    def dummy_func():
        pass

    # Mock inspect.getmodule and inspect.getfile
    mock_module = MagicMock()
    mock_module.__name__ = "toolviper.utils.dummy"

    with (
        patch("inspect.getmodule", return_value=mock_module),
        patch("inspect.getfile", return_value="/abs/path/src/toolviper/utils/dummy.py"),
    ):
        base, mod = get_path(dummy_func)
        assert "src/toolviper" in base
        assert mod == "/abs/path/src/toolviper/utils/dummy"


def test_set_config_directory(tmp_path):
    config_dir = tmp_path / "my_config"
    config_dir.mkdir()

    with patch("toolviper.utils.logger.info"):
        set_config_directory(str(config_dir))
        assert os.environ["PARAMETER_CONFIG_PATH"] == str(config_dir)


def test_validate_decorator_success(tmp_path, monkeypatch):
    # Setup a mock config file
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    param_file = config_dir / "test_mod.param.json"
    schema = {
        "my_func": {"arg1": {"type": "int", "required": True}, "arg2": {"type": "str"}}
    }
    with open(param_file, "w") as f:
        json.dump(schema, f)

    def my_func(arg1, arg2="default"):
        return f"{arg1}-{arg2}"

    # Manually wrap with validate and trick it
    my_func.__module__ = "toolviper.utils.test_mod"
    my_func.__name__ = "my_func"

    wrapped = validate(config_dir=str(config_dir))(my_func)

    # We also need to mock get_path to avoid it searching in /tmp or something
    with patch(
        "toolviper.utils.parameter.get_path",
        return_value=(str(tmp_path), str(tmp_path / "test_mod")),
    ):
        assert wrapped(10, arg2="hello") == "10-hello"


def test_validate_decorator_failure(tmp_path):
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    param_file = config_dir / "test_mod.param.json"
    schema = {"fail_func": {"arg1": {"type": "int"}}}
    with open(param_file, "w") as f:
        json.dump(schema, f)

    def fail_func(arg1):
        return arg1

    fail_func.__module__ = "toolviper.utils.test_mod"
    fail_func.__name__ = "fail_func"

    wrapped = validate(config_dir=str(config_dir))(fail_func)

    with patch(
        "toolviper.utils.parameter.get_path",
        return_value=(str(tmp_path), str(tmp_path / "test_mod")),
    ):
        # Should raise AssertionError from verify's assert validator.validate(args)
        with pytest.raises(AssertionError):
            wrapped("not an int")


def test_verify_missing_config():
    def no_config_func():
        pass

    no_config_func.__module__ = "ghost_module"

    with (
        patch(
            "toolviper.utils.parameter.get_path",
            return_value=("/tmp", "/tmp/ghost_module"),
        ),
        patch("toolviper.utils.logger.error"),
    ):
        with pytest.raises(FileNotFoundError):
            verify(
                no_config_func,
                {},
                {"function": "no_config_func", "module": "ghost_module"},
            )


def test_verify_function_not_in_schema(tmp_path):
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    param_file = config_dir / "known_mod.param.json"
    with open(param_file, "w") as f:
        json.dump({"other_func": {}}, f)

    def unknown_func():
        pass

    unknown_func.__module__ = "known_mod"

    with patch(
        "toolviper.utils.parameter.get_path",
        return_value=(str(tmp_path), str(tmp_path / "known_mod")),
    ):
        with pytest.raises(KeyError):
            verify(
                unknown_func,
                {},
                {"function": "unknown_func", "module": "known_mod"},
                config_dir=str(config_dir),
            )

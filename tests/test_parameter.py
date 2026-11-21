import json
import os
import subprocess
import sys
from unittest.mock import MagicMock, patch

import numpy
import pytest

from toolviper.utils.parameter import (
    ParameterError,
    get_path,
    is_notebook,
    set_config_directory,
    validate,
    verify,
)
from toolviper.utils.protego import Protego


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
        # ParameterError still derives from AssertionError, which verify raised
        # before it stopped using assert.
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


def test_validate_decorator_raises_parameter_error(tmp_path):
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    with open(config_dir / "test_mod.param.json", "w") as f:
        json.dump({"count_func": {"count": {"type": "int", "min": 1}}}, f)

    def count_func(count):
        return count

    count_func.__module__ = "toolviper.utils.test_mod"
    count_func.__name__ = "count_func"

    wrapped = validate(config_dir=str(config_dir))(count_func)

    with patch(
        "toolviper.utils.parameter.get_path",
        return_value=(str(tmp_path), str(tmp_path / "test_mod")),
    ):
        assert wrapped(numpy.int64(2)) == 2
        with pytest.raises(ParameterError) as excinfo:
            wrapped(True)

    assert isinstance(excinfo.value, ValueError)
    assert isinstance(excinfo.value, AssertionError)
    assert "count_func" in str(excinfo.value)


def test_validation_survives_python_optimize(tmp_path):
    # `python -O` strips assert statements; validation must still run.
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    schema = {"bounded": {"x": {"type": "float", "min": 0.0, "max": 1.0}}}
    (config_dir / "optimized_mod.param.json").write_text(json.dumps(schema))
    (tmp_path / "optimized_mod.py").write_text(
        "import toolviper.utils.parameter as parameter\n\n\n"
        f"@parameter.validate(config_dir={str(config_dir)!r})\n"
        "def bounded(x):\n"
        "    return x\n"
    )
    script = (
        "import sys\n"
        f"sys.path.insert(0, {str(tmp_path)!r})\n"
        "import optimized_mod\n"
        "try:\n"
        "    optimized_mod.bounded(1.5)\n"
        "    print('RESULT accepted')\n"
        "except Exception as exc:\n"
        "    print('RESULT', type(exc).__name__)\n"
    )
    result = subprocess.run(
        [sys.executable, "-O", "-c", script],
        capture_output=True,
        text=True,
        check=True,
    )
    outcome = [line for line in result.stdout.splitlines() if line.startswith("RESULT")]
    assert outcome == ["RESULT ParameterError"]


@pytest.mark.parametrize(
    ("type_name", "value", "valid"),
    [
        ("int", 3, True),
        ("int", numpy.int64(3), True),
        ("int", True, False),
        ("int", numpy.True_, False),
        ("int", 3.0, False),
        ("integer", numpy.uint8(3), True),
        ("integer", False, False),
        ("float", 0.5, True),
        ("float", 2, True),
        ("float", numpy.float32(0.5), True),
        ("float", numpy.float64(0.5), True),
        ("float", True, False),
        ("number", numpy.int32(1), True),
        ("number", True, False),
        ("boolean", True, True),
        ("boolean", numpy.True_, True),
        ("boolean", 1, False),
    ],
)
def test_numeric_and_boolean_types(type_name, value, valid):
    validator = Protego({"x": {"type": type_name}})
    assert validator.validate({"x": value}) is valid


def test_range_alone_accepts_nan():
    # Every comparison with NaN is false, so min and max cannot reject it.
    validator = Protego({"x": {"type": "float", "min": 0.0, "max": 1.0}})
    assert validator.validate({"x": float("nan")})


@pytest.mark.parametrize(
    ("value", "valid"),
    [
        (0.5, True),
        (3, True),
        (float("nan"), False),
        (float("inf"), False),
        (-float("inf"), False),
        (numpy.float32("nan"), False),
        (complex(1.0, float("nan")), False),
        ([0.5, 1.5], True),
        ([0.5, float("nan")], False),
        ((1.0, float("inf")), False),
        (numpy.array([0.5, 1.5]), True),
        (numpy.array([0.5, numpy.nan]), False),
        (numpy.array([1, 2]), True),
    ],
)
def test_finite_rule(value, valid):
    validator = Protego({"x": {"finite": True}})
    assert validator.validate({"x": value}) is valid


def test_finite_and_range_inside_dict_schema():
    validator = Protego(
        {
            "params": {
                "type": "dict",
                "allow_unknown": True,
                "schema": {
                    "robust": {
                        "type": "float",
                        "min": -2.0,
                        "max": 2.0,
                        "finite": True,
                    }
                },
            }
        }
    )
    assert validator.validate({"params": {"robust": 0.5, "other": "x"}})
    assert not validator.validate({"params": {"robust": float("nan")}})
    assert not validator.validate({"params": {"robust": 3.0}})

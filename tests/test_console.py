import pytest
import inspect
from toolviper.utils.console import ColorCodes, Colorize, add_verbose_info

def test_color_codes():
    codes = ColorCodes()
    assert codes.reset == "\033[0m"
    assert codes.red == "\033[38;2;220;20;60m"

def test_colorize_basic():
    c = Colorize()
    text = "hello"
    assert c.bold(text) == f"\033[1m{text}\033[0m"
    assert c.red(text) == f"\033[38;2;220;20;60m{text}\033[0m"
    assert c.blue(text) == f"\033[38;2;50;50;205m{text}\033[0m"

def test_colorize_format_list():
    c = Colorize()
    # Testing format with RGB list
    formatted = c.format("test", color=[255, 0, 0])
    assert "test" in formatted
    assert "38;2;255;0;0" in formatted

def test_colorize_format_string():
    c = Colorize()
    formatted = c.format("test", color="green")
    assert "test" in formatted
    assert "38;2;46;139;87" in formatted

def test_get_color_function():
    c = Colorize()
    fn = c.get_color_function("red")
    assert fn == c.red
    
    # Default to black if not found
    fn_unknown = c.get_color_function("nonexistent")
    assert fn_unknown == c.black

def test_add_verbose_info():
    def dummy_caller():
        # result = add_verbose_info("my message")
        # In this context, dummy_caller is the direct caller, so PENULTIMATE_FUNCTION (2) 
        # might refer to the caller of dummy_caller if add_verbose_info is called from it.
        # Actually, add_verbose_info uses PENULTIMATE_FUNCTION = 2.
        # stack[0] = add_verbose_info
        # stack[1] = dummy_caller
        # stack[2] = test_add_verbose_info
        return add_verbose_info("my message")
    
    result = dummy_caller()
    # It seems in this pytest execution, it gets 'test_add_verbose_info' as PENULTIMATE_FUNCTION
    assert "my message" in result
    assert "\033[" in result

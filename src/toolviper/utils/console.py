import inspect

from dataclasses import dataclass
from typing import List, Union, Callable

PREVIOUS_FUNCTION = 1
PENULTIMATE_FUNCTION = 2


@dataclass
class ColorCodes:
    # Color formatting
    black: str = "\033[38;2;0;0;0m"
    grey: str = "\033[38;2;112;128;144m"

    red: str = "\033[38;2;220;20;60m"
    green: str = "\033[38;2;46;139;87m"
    yellow: str = "\033[38;2;245;200;30m"
    orange: str = "\033[38;2;255;160;0m"
    blue: str = "\033[38;2;50;50;205m"
    purple: str = "\033[38;2;128;05;128m"
    white: str = "\033[38;2;245;255;250m"

    # Text formatting
    bold: str = "\033[1m"
    faint: str = "\033[2m"
    italics: str = "\033[3m"
    underline: str = "\033[4m"
    blink: str = "\033[5m"
    highlight: str = "\033[7m"

    reset: str = "\033[0m"
    alert: str = "\033[7;38;2;220;60;20m"


class Colorize:
    def __init__(self):
        self.codes = ColorCodes()

        self.levels = {"info": self.blue}

    def bold(self, text: str) -> str:
        return "{format}{text}{reset}".format(
            format=self.codes.bold, text=text, reset=self.codes.reset
        )

    def faint(self, text: str) -> str:
        return "{format}{text}{reset}".format(
            format=self.codes.faint, text=text, reset=self.codes.reset
        )

    def italics(self, text: str) -> str:
        return "{format}{text}{reset}".format(
            format=self.codes.italics, text=text, reset=self.codes.reset
        )

    def underline(self, text: str) -> str:
        return "{format}{text}{reset}".format(
            format=self.codes.underline, text=text, reset=self.codes.reset
        )

    def blink(self, text: str) -> str:
        return "{format}{text}{reset}".format(
            format=self.codes.blink, text=text, reset=self.codes.reset
        )

    def highlight(self, text: str) -> str:
        return "{format}{text}{reset}".format(
            format=self.codes.highlight, text=text, reset=self.codes.reset
        )

    def white(self, text: str) -> str:
        return "{color}{text}{reset}".format(
            color=self.codes.white, text=text, reset=self.codes.reset
        )

    def black(self, text: str) -> str:
        return "{color}{text}{reset}".format(
            color=self.codes.black, text=text, reset=self.codes.reset
        )

    def grey(self, text: str) -> str:
        return "{color}{text}{reset}".format(
            color=self.codes.grey, text=text, reset=self.codes.reset
        )

    def red(self, text: str) -> str:
        return "{color}{text}{reset}".format(
            color=self.codes.red, text=text, reset=self.codes.reset
        )

    def green(self, text: str) -> str:
        return "{color}{text}{reset}".format(
            color=self.codes.green, text=text, reset=self.codes.reset
        )

    def yellow(self, text: str) -> str:
        return "{color}{text}{reset}".format(
            color=self.codes.yellow, text=text, reset=self.codes.reset
        )

    def orange(self, text: str) -> str:
        return "{color}{text}{reset}".format(
            color=self.codes.orange, text=text, reset=self.codes.reset
        )

    def blue(self, text: str) -> str:
        return "{color}{text}{reset}".format(
            color=self.codes.blue, text=text, reset=self.codes.reset
        )

    def purple(self, text: str) -> str:
        return "{color}{text}{reset}".format(
            color=self.codes.purple, text=text, reset=self.codes.reset
        )

    def alert(self, text: str) -> str:
        return "{color}{text}{reset}".format(
            color=self.codes.alert, text=text, reset=self.codes.reset
        )

    @staticmethod
    def from_ansi(
        color: Union[str, List],
        bold: bool = False,
        italics: bool = False,
        faint: bool = False,
        underline: bool = False,
        highlight: bool = False,
        blink: bool = False,
    ):
        args = locals()
        args.pop("color")

        escape_code = "\033["

        if True not in args.values():
            escape_code = "".join((escape_code, "0"))

        else:
            option_list = []

            if bold:
                option_list.append("1")

            if faint:
                option_list.append("2")

            if italics:
                option_list.append("3")

            if underline:
                option_list.append("4")

            if blink:
                option_list.append("5")

            if highlight:
                option_list.append("7")

            option_string = ";".join(option_list)
            escape_code = "".join((escape_code, option_string))

        escape_code = ";".join((escape_code, "38;2"))
        color_code = ";".join(map(str, color)) + "m"

        return ";".join((escape_code, color_code))

    def format(
        self,
        text: str,
        color: Union[List, str],
        bold: bool = False,
        italics: bool = False,
        faint: bool = False,
        underline: bool = False,
        highlight: bool = False,
        blink: bool = False,
    ):
        if isinstance(color, list):
            return "{color}{text}{reset}".format(
                color=self.from_ansi(
                    color=color,
                    bold=bold,
                    italics=italics,
                    faint=faint,
                    underline=underline,
                    highlight=highlight,
                    blink=blink,
                ),
                text=text,
                reset=self.codes.reset,
            )
        else:
            ascii_list = self.get_color_ascii_list(color=color)
            return "{color}{text}{reset}".format(
                color=self.from_ansi(
                    color=ascii_list,
                    bold=bold,
                    italics=italics,
                    faint=faint,
                    underline=underline,
                    highlight=highlight,
                    blink=blink,
                ),
                text=text,
                reset=self.codes.reset,
            )

    def get_color_function(self, color: str) -> Callable:
        import inspect

        for method in inspect.getmembers(self):
            if color is method[0]:
                return getattr(self, method[0])

        return self.black

    def get_color_ascii_list(self, color: str):
        import re
        import inspect

        for member in inspect.getmembers(self.codes):
            if member[0] == color:
                return re.split("[;m]", member[1])[-4:-1]


def add_verbose_info(message: str, color: str = "blue") -> str:
    function_name = inspect.stack()[PENULTIMATE_FUNCTION].function
    colorize = Colorize()
    color_function = colorize.get_color_function(color=color)

    return "[{function_name}]: {message}".format(
        function_name=color_function(function_name), message=message
    )

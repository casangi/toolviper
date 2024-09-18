import os
import glob
import json
import pkgutil
import pathlib
import inspect

import functools
import importlib

import toolviper.utils.logger
import toolviper.utils.console as console

from toolviper.utils.protego import Protego

from typing import Callable, Any, Union, NoReturn, Dict, List, Optional, Tuple
from types import ModuleType


def is_notebook() -> bool:
    try:
        from IPython import get_ipython

        shell = get_ipython().__class__.__name__
        if shell == "ZMQInteractiveShell":
            return True
        else:
            raise ImportError

    except ImportError:
        return False


def validate(
        config_dir: str = None,
        custom_checker: Callable = None,
        add_data_type: Any = None,
        external_logger: Callable = None,
):
    def function_wrapper(function):
        @functools.wraps(function)
        def wrapper(*args, **kwargs):
            meta_data = {"function": None, "module": None}
            arguments = inspect.getcallargs(function, *args, **kwargs)

            meta_data["function"] = function.__name__
            meta_data["module"] = function.__module__

            # If this is a class method, drop the self entry.
            if "self" in list(arguments.keys()):
                class_name = args[0].__class__.__name__
                meta_data["function"] = ".".join((class_name, function.__name__))
                del arguments["self"]

            verify(
                function=function,
                args=arguments,
                config_dir=config_dir,
                custom_checker=custom_checker,
                add_data_type=add_data_type,
                external_logger=external_logger,
                meta_data=meta_data,
            )

            return function(*args, **kwargs)

        return wrapper

    return function_wrapper


# DEPRECATED
def _get_path(function: Callable) -> str:
    module = inspect.getmodule(function)
    module_path = inspect.getfile(module).rstrip(".py")

    if "src" in module_path:
        # This represents a local developer install

        base_module_path = module_path.split("src/")[0]
        return base_module_path

    else:
        # Here we hope that we can find the package in site-packages and it is unique
        # otherwise the user should provide the configuration path in the decorator.

        base_module_path = module_path.split("site-packages/")[0]
        return str(pathlib.Path(base_module_path).joinpath("site-packages/"))


def get_path(function: Callable) -> tuple[str, str]:
    module = inspect.getmodule(function)
    module_path = inspect.getfile(module).removesuffix(".py")

    # Determine whether this is a developer install or a site install
    tag = "src" if "src" in module_path else "site-packages"

    try:
        # The base directory should be to the left of the tag
        base_module_path = module_path.split(f"{tag}/")[0]

        # Split the module path up and determine what the package name and location is.
        split_path = module_path.split("/")
        index = split_path.index(tag) + 1
        package_name = split_path[index]

        # Build the full package path
        base_module_path = pathlib.Path(base_module_path).joinpath(f"{tag}/{package_name}")

        return str(base_module_path), module_path

    except ValueError:
        import importlib
        toolviper.utils.logger.debug(f"module {module.__name__} has non-standard install path ...")

        # Need to get the base package name
        package = module.__name__.split(".")[0]

        # IF we load the base pacakge we can find the base path
        package_path = importlib.import_module(package).__file__
        base_module_path = pathlib.Path(package_path).parent

        return str(base_module_path), module_path


def config_search(root: str = "/", module_name=None) -> Union[None, str]:
    colorize = console.Colorize()

    if root == "/":
        toolviper.utils.logger.warning(
            "File search from root could take some time ..."
        )

    toolviper.utils.logger.debug(
        "Searching {} for configuration file, please wait ...".format(
            colorize.blue(root)
        )
    )

    for file in glob.glob("{root}/**".format(root=root), recursive=True):
        if module_name + ".param.json" in file:
            basename = os.path.dirname(file)
            return basename

    return None


def set_config_directory(path: str, create: bool = False) -> NoReturn:
    colorize = console.Colorize()
    if pathlib.Path(path).exists():
        toolviper.utils.logger.info(
            "Setting configuration directory to [{path}]".format(
                path=colorize.blue(path)
            )
        )
        os.environ["PARAMETER_CONFIG_PATH"] = path
    else:
        toolviper.utils.logger.info(
            "The configuration directory [{path}] does not currently exist.".format(
                path=colorize.blue(path)
            )
        )
        if create:
            toolviper.utils.logger.info(
                "Creating empty configuration directory: {path}".format(
                    path=colorize.blue(path)
                )
            )
            pathlib.Path(path).mkdir()


def verify_configuration(path: str, module: ModuleType) -> List[str]:
    modules = []
    for file in glob.glob("{path}/*.param.json".format(path=path), recursive=True):
        if file.endswith(".param.json"):
            modules.append(os.path.basename(file).strip(".param.json"))

    package_path = os.path.dirname(module.__file__)
    package_modules = [name for _, name, _ in pkgutil.iter_modules([package_path])]

    not_found = []
    for module in package_modules:
        if module not in modules:
            not_found.append(module)

    # coverage = (len(package_modules) - len(not_found)) / len(package_modules)

    return package_modules


def verify(
        function: Callable,
        args: Dict,
        meta_data: Dict[str, Union[Optional[str], Any]],
        config_dir: str = None,
        add_data_type: Any = None,
        custom_checker: Callable = None,
        external_logger: Callable = None,
) -> NoReturn:
    colorize = console.Colorize()
    function_name, module_name = meta_data.values()

    # The module call gives the full module chain, but we only want the last
    # module, ex. astrohack.extract_holog would be returned, but we only want
    # extract_holog. This should generally work.

    module_name = module_name.split(".")[-1]

    if external_logger is None:
        logger = toolviper.utils.logger.get_logger()

    else:
        logger = external_logger

    toolviper.utils.logger.debug(
        "Checking parameter values for {module}.{function}".format(
            function=colorize.blue(function_name), module=colorize.blue(module_name)
        )
    )

    path = None

    package_path, module_path = get_path(function)
    logger.info(f"Module path: {colorize.blue(package_path)}")

    # First we need to find the parameter configuration files
    if pathlib.Path(package_path).joinpath("config").joinpath(f"{module_name}.param.json").exists():
        logger.debug(f"Found configuration for {module_name}.{function_name} in: {colorize.blue(package_path)}")
        path = str(pathlib.Path(package_path).joinpath("config"))

    # User specified configuration directory take precedent
    if config_dir is not None:
        if pathlib.Path(config_dir).joinpath(f"{module_name}.param.json").exists():
            logger.debug(f"Setting configuration directory to user provided [{config_dir}]")
            path = config_dir

        else:
            logger.warning("User provided configuration directory does not exist. Searching for parameter files ...")

    # If we have been delt only failure at this point, then we try one last ditch effort. Search the package directory!
    if not path:
        logger.debug(f"Couldn't determine parameter configuration directory, doing a depth search of {package_path}")
        path = config_search(root=package_path, module_name=module_name)

        if not path:
            logger.error(f"Cannot find parameter configuration directory for {function_name}")
            raise FileNotFoundError

    # Define parameter file name
    parameter_file = module_name + ".param.json"

    logger.debug(f"Parameter configuration file: {pathlib.Path(path).joinpath(parameter_file)}")

    with open(pathlib.Path(path).joinpath(parameter_file)) as json_file:
        schema = json.load(json_file)

    if function_name not in schema.keys():
        logger.error(
            "{function} not_found in parameter configuration files.".format(
                function=colorize.format(function_name, color="red", bold=True)
            )
        )

        raise KeyError

    # Here is where the parameter checking is done.
    # First instantiate the validator.
    validator = Protego()

    # Register any additional data types that might be in the configuration file.
    if add_data_type is not None:
        validator.register_data_type(add_data_type)

    # Set up the schema to validate against.
    validator.schema = schema[function_name]

    # If a custom unit custom checker is needed, instantiate the
    # void function in the validator class. In the schema this
    # is used with "check allowed with".
    if custom_checker is not None:
        validator.custom_allowed_function = custom_checker

    assert validator.validate(args), logger.error(validator.errors)

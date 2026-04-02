## Parameter Verification Module Users Guide

The `parameter` module is based off of another well known verification module called [cerberus](https://docs.python-cerberus.org/). Much of the syntax is the same but there is some added functionality specific to the viper and radps frameworks that is only available using `parameter`. A detailed list of validation type that come standard to the `cerberus` package can be found [here](https://docs.python-cerberus.org/validation-rules.html). As a note, custom types can be added as well as detailed below.

### Parameter Configuration File
The parameter configuration files layout the validation rules for each function in a given module and one must be included for any module that requires validation. The configuration files should be placed in `src/toolviper/config`. The filename should be of the form, `<module_name>.param.json` and is a standard form json file witht he following layout.
```
{
    "function-name-1":{
        function-parameter-1:{
            "parameter-rule-1": ...,
            "parameter-rule-2": ...,
            ...,
            "parameter-rule-n": ...
        },
        function-parameter-2:{
            "parameter-rule-1": ...,
            "parameter-rule-2": ...,
            ...,
            "parameter-rule-n": ...
        },...
    },
   "function-name-2":{
       ...
    }
}
```

In this way, we can define validation rules for each parameter of each function in a module. A number of examples can be seen in the `astrohack` repository [here](https://github.com/nrao/astrohack/tree/astrohack-dev/src/astrohack/config). Let's consdier a simple example below. A function prints a name and an age but allows for the age to be defined either as an integer or a string.

```
[database.py]

def entry(name: str, age: Union[str, int]):
    print(f"{name}: {age}")
```

In order to add validation to this function we would simply add the validation decorator and define the configuration file as seen below,

```
[database.py]

@toolviper.utils.parameter.validate()
def entry(name: str, age: Union[str, int]):
    print(f"{name}: {age}")
```
Then we define a simple configuration file in the following way,
```
[database.param.json]

{
    "entry":{
        "name":{
            "type": "string",
            "required": True
        },
        "age":{
            "type": ["string", "integer"],
            "required": True
        }
    }
}
```

This will require each parameter to be included as well as enforce the data type of each input parameter. There are many more useless restrictions that can be added to input parameter by referring to the documentaion. Also, custom rules can be made by opening a ticket on the `toolviper` repository under "new feature". Currently, in addition to the standard parameter rules there are two additional features that have been added. The `sequence` and `struct type` parameter checks. The former checks that a set of inputs are in a given order and the latter works the same as the `type` keword except it defines a type for values that make up a structure. As an example, if we wanted to set a tpye restriction on a list of ddi values to require that they be a list of integers or strings we would use the keyword `struct type` as so to define a parameter rule.

```
"struct type": ["integer", "string"]
```

### Parameter Checking for Class Members
The addition of the parameter module to members of a class requires only a few simple changes. Let's consider the databse example from above and make the entry function a class member of the ``Database`` class instead of just being part of the module.

```
[database.py]

class Database:
    def __init__(self):
        self.name = None
        self.age = None

    def entry(self, name: str, age: Union[str, int]):
        print(f"{name}: {age}")
```

In this case the changes to the configuration file are simple. We need only make a small change to how we define the function-name section.

```
[database.param.json]

{
    "Database.entry":{
        "name":{
            "type": "string",
            "required": True
        },
        "age":{
            "type": ["string", "integer"],
            "required": True
        }
    }
}
```

and then the class members can be validated by simply adding the decorator to the class members you want to evaluate. No additional changes are necessary.

```
[database.py]

class Database:
    def __init__(self):
        self.name = None
        self.age = None

    @toolviper.utils.parameter.validate()
    def entry(self, name: str, age: Union[str, int]):
        print(f"{name}: {age}")
```

### Optional Details

The parameter checking is applied to any function decorated with the function `toolviper.utils.parameter.validate(...)`. The verification
function has the following input parameters available.

- `config_dir`: This specifies the configuration directory
- `logger`: This allows the user to pass a specific logger instance to the parameter checking. If this is not done, `toolviper`
  will spawn its own internal logger. The only difference will be the logger name in the output.
- `add_data_type`: Not all data types are available from the default setup, more importantly though there are a number of
  instances in the framework(s) where custom data objects are passed to functions and there needs to be a way to check these
  as well. This allows the user to register a custom data type for checking. All that is needed is to pass an instance of
  the data type.
- `custom_checker`: This allows the user to register a function that will return all allowed data types for a parameter.

Taking the example from the code, to implement the verification on the `snake_danger_checker(...)` function, assuming the
configuration directory is set up in `__init__.py`:

### Another Standard Function Implementation Example:
```angular2html
import toolviper

@toolviper.utils.parameter.validate
def snake_danger_checker(number: int, poison: bool, species: str)->Union[int, NoReturn]:
    ...
```

The configuration file for this would be as follows (though the user could add more layers)

```angular2html
snake/config/viper.param.json:

{
  "snake_danger_checker":{
    "number": {
      "required": true,
      "type": ["int"]
    },
    "poison": {
      "required": true,
      "type": "boolean"
    },
    "species": {
      "required": true,
      "type": "string"
    }
  }
```

If this was a checker on a class method the only change to be made would be that that function name would change to
`ClassName.snake_danger_checker`.

This is only a very sparse version of a configuration file, the requirements can be made quite strict with some work. A
full list of supported parameter checks can be found at [cerberus::validation](https://docs.python-cerberus.org/validation-rules.html).
In addition, `toolviper` also supports:

- Custom data types: `ndarray` is supported by default in `toolviper` and any valid data type can be registered
- Custom checking function
- Sequence validation
- Structure data type

The framework is fully extensible so additional checks can be added without too much work.

### Function with Custom Data Implementation:

Extending our previous example, let's say we have a custom data object that we can use to dynamically store snake
information, and we want to pass this instead. If we define the object as follows,

```angular2html
snake_info = snake.viper.SnakeObject(
    object_name="snake-info-list",
    poison=True,
    species="cobra",
    color="brown",
    angry=True
)
```

We can use this in a modified snake checking function and verify it by simply passing an instance of the `SnakeObject` to
the validation using the `add_data_type` parameter.

```angular2html
@toolviper.utils.parameter.validate(
    logger=toolviper.utils.logger.get_logger(logger_name="viper-logger"),
    add_data_type=SnakeObject
)
def snake_object_danger_checker(number: int, snake_info: SnakeObject) -> Union[float, NoReturn]:
    ...
```

### Custom Data Checker

This option is for the case that the user wants to define a function that will use some sort of custom logic to decide the
acceptable values for a given parameter and then return them to the validation scheme. The best example of this is in `astrohack`
where some of the plotting functions have a different set of acceptable units depending on the plot being made, therefore
we want an easily modifiable list of units that can be returned instead of hard-coding each function separately. The primary
requirement for a custom checking function is that it take a string parameter and return a list of acceptable values to
check against. In the case of `astrohack` this was a list of acceptable plotting units depending on whether the function
needs trigonometric, time or radian units. The custom check function in this case was,

```angular2html
def custom_unit_checker(unit_type):
    if unit_type == "units.trig":
        return trigo_units

    elif unit_type == "units.length":
        return length_units

    elif unit_type == "units.time":
        return time_units

    else:
        return "Not found"
```

where the units are defined in a constants module as,

```angular2html
# Length units
length_units = ['km', 'mi', 'm', 'yd', 'ft', 'in', 'cm', 'mm', 'um', 'mils']

# Trigonometric units
trigo_units = ['rad', 'deg', 'hour', 'asec', 'amin']

# Time units
time_units = ['nsec', 'usec', 'msec', 'sec', 'min', 'hour', 'day']
```

In order to add this to the plotting function

```angular2html
 @toolviper.utils.parameter.validate(
        logger=logger.get_logger(logger_name="astrohack"),
        custom_checker=custom_unit_checker
    )
    def plot_array_configuration(
            self,
            destination: str,
            stations: bool = True,
            zoff: bool = False,
            unit: str = 'm',
            box_size: Union[int, float] = 5000,
            display: bool = False,
            figure_size: Union[Tuple, List[float], np.array] = None,
            dpi: int = 300
    ) -> None:
    ...
```

and in the configuration file this function has the following parameter check for the unit parameter,

```angular2html
...
    "unit":{
            "nullable": false,
            "required": false,
            "type": ["string"],
            "check allowed with": "units.length"
    },
...
```

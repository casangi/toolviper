## Parameter Verification Module Users Guide

The `parameter` module is based off of another well known verification module called [cerberus](https://docs.python-cerberus.org/). Much of the syntax is the same but there is some added functionality specific to the viper and radps frameworks that is only available using `parameter`. A detailed list of validation type that come standard to the `cerberus` package can be found [here](https://docs.python-cerberus.org/validation-rules.html). As a note, custom types can be added as well as detailed below.

### Parameter Configuration File
The parameter configuration files latyout the validation rules for eavh function in a given module and one must be included for the any module that requires validation. The configuration files should be placed in `src/toolviper/config`. The filename should be of the form, `<module_name>.param.json` and is a standard form json file witht he followinf layout.
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

This will require each parameter to be included as well as enforce the data type of each input parameter. There are many more useless restrictions that can be added to input parameter by referring to the documentaion. Also, custom rules can be made by opening a ticket on the `toolviper` repository under "new feature". Currently, in addition to the standard parameter rules there are two additional features that have been added. The `sequence` and `struct type` parameter checks. The former checsk that a set of inputs are in a given order and the latter works the same as the `type` keword except it defines a type for values that make up a structure. As an example, if we wanted to set a tpye restriction on a list of ddi values to require that they be a list of integers or stirngs we would use the keyword `struct type` as so to define a parameter rule.

```
"struct type": ["integer", "string"]
```

### Parameter Checking for Class Members
The addition of the paramter module to members of of a class requires only a few simple changes. Let's consider the databse example from above and make the entry funtion a class member of the Database class instead of just being part of the module.

```
[database.py]

class Database:
    def __init__(self):
        self.name = None
        self.age = None

    def entry(self, name: str, age: Union[str, int]):
        print(f"{name}: {age}")
```

In this case the changes to the configuration file are simple. We need only make a small change to how we define the funtion-name section.

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
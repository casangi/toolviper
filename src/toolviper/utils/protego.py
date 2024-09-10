import numpy
import cerberus


class Protego(cerberus.Validator):
    numpy_ndarray_type = cerberus.TypeDefinition("ndarray", (numpy.ndarray,), ())
    str_type = cerberus.TypeDefinition("str", (str,), ())
    int_type = cerberus.TypeDefinition("int", (int,), ())
    tuple_type = cerberus.TypeDefinition("tuple", (tuple,), ())

    types_mapping = cerberus.Validator.types_mapping.copy()
    types_mapping["ndarray"] = numpy_ndarray_type
    types_mapping["str"] = str_type
    types_mapping["int"] = int_type
    types_mapping["tuple"] = tuple_type

    @staticmethod
    def custom_allowed_function(*args, **kwargs):
        return None

    def _validate_check_allowed_with(self, constraint, field, value):
        """Test the custom constraint

        The rule's arguments are validated against this schema:
        {'type': 'string'}
        """
        custom_constraint = self.custom_allowed_function(constraint)
        if custom_constraint is None:
            self._error(field, "Custom constraint checking function not valid")
            return

        if isinstance(value, list):
            for entry in value:
                if entry not in custom_constraint:
                    self._error(
                        field,
                        "Allowed values not satisfied: {value} != {c}".format(
                            value=value, c=custom_constraint
                        ),
                    )
        else:
            if value not in custom_constraint:
                self._error(
                    field,
                    "Allowed values not satisfied: {value} != {c}".format(
                        value=value, c=custom_constraint
                    ),
                )

    def _validate_sequence(self, constraint, field, value):
        """Test the custom constraint

        The rule's arguments are validated against this schema:
        {'type': 'list'}
        """
        if not value == constraint:
            self._error(
                field,
                "Correct sequence not satisfied: {value}!={constraint}".format(
                    value=value, constraint=constraint
                ),
            )

    def _validate_struct_type(self, constraint, field, value):
        """Test the structure type.

        constraint: True | False
        field: amount
        value: number

        The rule's arguments are validated against this schema:
        {'type': 'list'}
        """
        if isinstance(value, list):
            for entry in value:
                if not type(entry).__name__ in constraint:
                    self._error(
                        field,
                        "Must be of type:{constraint}".format(constraint=constraint),
                    )

    def register_data_type(self, data_type):
        new_data_type = cerberus.TypeDefinition(data_type.__name__, (data_type,), ())
        self.types_mapping[data_type.__name__] = new_data_type

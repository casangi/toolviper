import cmath
import numbers

import cerberus
import numpy

# NumPy scalars are not subclasses of the Python number types (numpy.int64 is
# not an int, numpy.float32 is not a float), while bool is a subclass of int.
# The numeric types below accept NumPy scalars and reject booleans.
_BOOLEAN_TYPES = (bool, numpy.bool_)
_INTEGER_TYPES = (int, numpy.integer)
_REAL_TYPES = (int, float, numpy.integer, numpy.floating)


class Protego(cerberus.Validator):
    numpy_ndarray_type = cerberus.TypeDefinition("ndarray", (numpy.ndarray,), ())
    str_type = cerberus.TypeDefinition("str", (str,), ())
    int_type = cerberus.TypeDefinition("int", _INTEGER_TYPES, _BOOLEAN_TYPES)
    tuple_type = cerberus.TypeDefinition("tuple", (tuple,), ())

    types_mapping = cerberus.Validator.types_mapping.copy()
    types_mapping["ndarray"] = numpy_ndarray_type
    types_mapping["str"] = str_type
    types_mapping["int"] = int_type
    types_mapping["tuple"] = tuple_type

    # Cerberus' own numeric and boolean types, made NumPy-aware. As in
    # Cerberus, "float" also accepts integers.
    types_mapping["integer"] = cerberus.TypeDefinition(
        "integer", _INTEGER_TYPES, _BOOLEAN_TYPES
    )
    types_mapping["float"] = cerberus.TypeDefinition(
        "float", _REAL_TYPES, _BOOLEAN_TYPES
    )
    types_mapping["number"] = cerberus.TypeDefinition(
        "number", _REAL_TYPES, _BOOLEAN_TYPES
    )
    types_mapping["boolean"] = cerberus.TypeDefinition("boolean", _BOOLEAN_TYPES, ())

    @staticmethod
    def custom_allowed_function(*args, **kwargs):
        """
        This is a dummy function. It returns None when not externally set in the validator.
        """
        return None

    def _validate_check_allowed_with(self, constraint, field, value):
        """Test the custom constraint

        The rule's arguments are validated against this schema:
        {'type': 'string'}
        """

        # This does nothing as long as the user does not specifically set the custom_allowed_function
        # in the validation code. So it seems useless bit is not.

        custom_constraint = self.custom_allowed_function(constraint)
        if custom_constraint is None:
            self._error(field, "Custom constraint checking function not valid")
            return

        if isinstance(value, list):
            for entry in value:
                if entry not in custom_constraint:
                    self._error(
                        field,
                        f"Allowed values not satisfied: {value} != {custom_constraint}",
                    )
        else:
            if value not in custom_constraint:
                self._error(
                    field,
                    f"Allowed values not satisfied: {value} != {custom_constraint}",
                )

    def _validate_finite(self, constraint, field, value):
        """Reject NaN and infinite values.

        Checks a number (Python or NumPy, real or complex), each number in a
        list or tuple, and every element of a floating-point or complex NumPy
        array. Other values are left to the ``type`` rule.

        The rule's arguments are validated against this schema:
        {'type': 'boolean'}
        """
        if not constraint:
            return

        if isinstance(value, numpy.ndarray):
            if value.dtype.kind in "fc" and not numpy.isfinite(value).all():
                self._error(field, "Must be finite (no NaN or infinity)")
            return

        entries = value if isinstance(value, list | tuple) else (value,)
        for entry in entries:
            # Booleans and integers are always finite.
            if isinstance(entry, numbers.Integral | numpy.bool_):
                continue
            if isinstance(entry, numbers.Complex) and not cmath.isfinite(entry):
                self._error(field, "Must be finite (no NaN or infinity)")
                return

    def _validate_sequence(self, constraint, field, value):
        """Test the custom constraint

        The rule's arguments are validated against this schema:
        {'type': 'list'}
        """
        if not value == constraint:
            self._error(
                field,
                f"Correct sequence not satisfied: {value}!={constraint}",
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
                if type(entry).__name__ not in constraint:
                    self._error(
                        field,
                        f"Must be of type:{constraint}",
                    )

    def register_data_type(self, data_type):
        new_data_type = cerberus.TypeDefinition(data_type.__name__, (data_type,), ())
        self.types_mapping[data_type.__name__] = new_data_type

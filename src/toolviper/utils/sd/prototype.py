import itertools
import typing

import dask
import numpy as np
import xarray as xr


# Build a simple Dask dataset based on a given set of axes
def simulate(field, spw, polarization, antenna, row):
    data_shape = {
        "field": [i for i in range(field)],
        "spw": [i for i in range(spw)],
        "polarization": polarization,
        "antenna": [f"antenna_{i}" for i in range(antenna)],
        "row": [i for i in range(row)],
    }

    dataset = xr.Dataset(
        coords=data_shape,
        data_vars=dict(
            DATA=(
                list(data_shape.keys()),
                np.zeros((field, spw, len(polarization), antenna, row)),
            )
        ),
    )

    return dataset


def distribute(
    job: dict, axes: list[str], function: typing.Callable, previous=None
) -> list[dask.delayed]:
    """
    Distribute a function across a dataset along specified axes.

    This function creates a list of delayed dask tasks, where each task
    represents a call to the specified function with the dataset or previous
    result, and the values of the distribution axes.

    Parameters
    ----------
    dataset : xr.Dataset
        The input dataset to be distributed.
    axes : typing.List[str]
        The axes to distribute along.
    function : typing.Callable
        The function to be applied in a delayed manner.
    previous : typing.Any, optional
        A previous result to be passed to the function. Defaults to None.

    Returns
    -------
    typing.List[dask.delayed]
        A list of dask delayed objects.
    """
    # Get the coordinate values for each axis
    axis_values = [job["dataset"].coords[axis].values for axis in axes]
    # arguments = {axis: job["dataset"].coords[axis].values for axis in axes}
    # inputs = [dict(zip(axes, combo)) for combo in itertools.product(*arguments.values())]

    if isinstance(previous, list):
        axis_values.append(previous)
        # inputs.append(previous)

    # Create a delayed version of the function
    delayed_func = dask.delayed(function)

    # Use itertools.product to generate all combinations of axis values
    # and create a delayed task for each combination.
    # The axis values are passed as positional arguments after 'previous'.

    return [
        delayed_func(*values) if previous is not None else delayed_func(*values)
        for values in itertools.product(*axis_values)  # previously axis_values
    ]
    # output = []
    # for values in inputs:
    #    print(values)
    #    if isinstance(values, dict) and previous is not None:
    #        print("===== dict")
    #        output.append(delayed_func(**values))

    #    elif isinstance(values, list):
    #        print("===== list")
    #        output.append(delayed_func(*values))

    #    else:
    #        print("===== idk")
    #        output.append(delayed_func(values))

    # return output

    # return [
    #    delayed_func(**values) if previous is not None else delayed_func(**values)
    #    for values in inputs # previously axis_values
    # ]

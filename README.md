# ToolVIPER

Tools and utilities for optimized radio astronomy processing using the VIPER framework.

[![Python 3.11 3.12 3.13](https://img.shields.io/badge/python-3.11%20%7C%203.12%20%7C%203.13-blue)](https://www.python.org/downloads/release/python-3130/)
[![Linux Tests](https://github.com/casangi/toolviper/actions/workflows/python-testing-linux.yml/badge.svg?branch=main)](https://github.com/casangi/toolviper/actions/workflows/python-testing-linux.yml?query=branch%3Amain)
[![macOS Tests](https://github.com/casangi/toolviper/actions/workflows/python-testing-macos.yml/badge.svg?branch=main)](https://github.com/casangi/toolviper/actions/workflows/python-testing-macos.yml?query=branch%3Amain)
[![ipynb Tests](https://github.com/casangi/toolviper/actions/workflows/run-ipynb.yml/badge.svg?branch=main)](https://github.com/casangi/toolviper/actions/workflows/run-ipynb.yml?query=branch%3Amain)
[![Version Status](https://img.shields.io/pypi/v/toolviper.svg)](https://pypi.python.org/pypi/toolviper/)

ToolVIPER provides a suite of high-level tools designed to simplify Dask cluster management, logging, profiling, and data distribution for radio astronomy applications. It is part of the VIPER (Very Large Interferometry Post-processing and Exploratory Research) ecosystem.

## Features

- **Dask Cluster Management**: Easily create and manage local or SLURM-based Dask clusters with optimized configurations.
- **Unified Logging**: Structured, colorized, and worker-aware logging system.
- **Resource Profiling**: Built-in decorators to monitor CPU and memory usage of functions.
- **Interactive Utilities**: Enhanced data display for Jupyter notebooks, including interactive JSON and HTML views for complex dictionaries.
- **Data Management**: Simple interface for downloading and managing external datasets from Cloudflare.
- **Parameter Validation**: Decorator-based system for validating function arguments using JSON schemas.
- **Graph Distribution**: Tools to distribute functions across datasets along specific axes using Dask.

## Installation

ToolVIPER requires Python 3.11, 3.12, or 3.13.

### Basic Installation
```bash
pip install toolviper
```

### With Optional Dependencies
ToolVIPER offers several optional dependency sets:
- `test`: For running tests and linting (`pytest`, `pre-commit`, etc.)
- `interactive`: For Jupyter/IPython support (`jupyterlab`, `matplotlib`, `ipympl`, etc.)
- `docs`: For building documentation (`sphinx`, `nbsphinx`, etc.)
- `all`: Installs all optional dependencies.

Example:
```bash
pip install "toolviper[interactive]"
```

## Quick Start

### 1. Dask Client Management
#### [Example Notebook](docs/client_tutorial.ipynb)
ToolVIPER simplifies setting up a Dask environment.

```python
from toolviper.dask.client import local_client

# Start a local Dask client with 4 workers
client = local_client(cores=4, memory_limit="8GB")
print(client.dashboard_link)
```

For SLURM clusters:
```python
from toolviper.dask.client import slurm_cluster_client

client = slurm_cluster_client(
    workers_per_node=1,
    cores_per_node=4,
    memory_per_node="16GB",
    number_of_nodes=2,
    queue="normal",
    interface="ib0",
    python_env_dir="/path/to/env",
    dask_local_dir="/tmp/dask",
    dask_log_dir="dask_logs"
)
```

### 2. Logging
#### [Example Notebook](docs/toolviper-logger-formatting-example.ipynb)
ToolVIPER provides a pre-configured logger that can be used throughout your application.

```python
from toolviper.utils import logger

logger.info("Initializing process...")
logger.warning("Low memory detected", verbose=True)
```

### 3. Profiling
Monitor your functions' resource consumption using decorators.

```python
from toolviper.utils.profile import cpu_usage

@cpu_usage(filename="my_profile.csv")
def heavy_computation():
    # Your code here
    pass

heavy_computation()
```

### 4. Interactive Data Exploration
In a Jupyter notebook, you can use `DataDict` for better visualization of nested dictionaries.

```python
from toolviper.utils.display import DataDict

my_data = {"level1": {"level2": {"key": "value"}}}
dd = DataDict(my_data)
dd.display()  # Renders an interactive JSON tree in Notebooks
```

### 5. Data Management
Access and download external datasets easily.

```python
import toolviper

# List available files
toolviper.utils.data.list_files()

# Get a python list available files
toolviper.utils.data.get_files()

# Download a specific dataset
toolviper.utils.data.download("example_dataset.zip", folder="./data")
```

### 6. Parameter Validation
A detailed write-up can be found here: [Parameter Validation](docs/parameter.md).
<p>Ensure your functions receive correct arguments using the `@validate` decorator.

```python
from toolviper.utils.parameter import validate

@validate()
def my_function(param1: int, param2: str):
    # This function will automatically validate arguments
    # against a corresponding .param.json schema.
    pass
```

## Development and Testing

To contribute or run tests locally:

1. Clone the repository:
   ```bash
   git clone https://github.com/casangi/toolviper.git
   cd toolviper
   ```

2. Install in editable mode with test dependencies:
   ```bash
   pip install -e ".[test]"
   ```

3. Install the pre-commit git hooks (ruff linting/formatting, nbstripout, repo hygiene checks — also enforced by CI):
   ```bash
   pre-commit install
   ```

4. Run tests:
   ```bash
   pytest tests
   ```

## License
ToolVIPER is licensed under the terms of the LICENSE file included in the root directory.

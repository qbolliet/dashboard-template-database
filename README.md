# Database schema builder for dashboard template

This directory contains utilities to build a specific generic database scheme from a table.
> Full documentation is available [here](https://qbolliet.github.io/dashboard-template-database/)

## Objectives

This package is built to create :
* a generic schema to store a a table as a database ;
* export this schema a relational DuckDB database.

## Organization

The repository is organized as follow :

* the `docs` folder contains all the package documentation and descriptions of the database scheme
* the `logs` folder contains the logging files associated with the builders
* the `notebooks` folder contains illustrative notebooks
* the `outputs` folder contains program outputs.
* the `parameters` folder contains default labels for naming columns.
* the `scripts` folder contains a script for building a database from the table and with the paramters listed in the `config.yaml` file at the root.

## Installation

### Package and dependencies

```bash
git clone https://github.com/qbolliet/dashboard-template-database.git
poetry build
pip install -e dist/dashboard_template_database-0.1.0-py3-none-any.whl
```

The package is then usable as any other python package.

### Parametrisation

File in the `config.yaml` file :
```yaml
INPUT_DATA : '../data/df_origin.csv'
OUTPUT_DATA : '../outputs/database.db'
THRESHOLD : 200
``` 

### Documentation

To visualize the documentation :
```
poetry install --with docs
```

```
mkdocs build --port 5000
```

## Usage

Here's an example of how to use the functions in the package:

```python
from bozio_wasmer_simulations import CaptationMarginaleSimulator

# Create a simulator object
simulator = CaptationMarginaleSimulator()

# Define a dictionary of reform parameters
reform_params = {
    'TYPE': 'fillon',
    'PARAMS': {
        'PLAFOND': 2.7,
        'TAUX_50_SALARIES_ET_PLUS': 0.35,
        'TAUX_MOINS_DE_50_SALARIES': 0.354
    }
}

# Simulate a reform
data_simul = simulator.simulate_reform(name='my_reform', reform_params=reform_params, year=2022, simulation_step_smic=0.1, simulation_max_smic=4)
``` 

## License

The package is licensed under the MIT License.
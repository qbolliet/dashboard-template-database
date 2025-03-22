# Modules de base
import json
import os
from pathlib import Path
import yaml

# Emplacement du fichier
FILE_PATH = Path(os.path.abspath(__file__))

# Importation des modules ad hoc
from dashboard_template_database.builders.tables import DuckdbTablesBuilder
from dashboard_template_database.storage.loader import Loader

# Chargement du fichier de configurations
with open(os.path.join(FILE_PATH.parents[1], "config.yaml")) as file:
    config = yaml.safe_load(file)

# Chargement du fichier de parmaètres
with open(os.path.join(FILE_PATH.parents[1], "parameters/labels.json")) as file:
    labels = json.load(file)

# Construction du schéma et de la base de données
if __name__ == '__main__' :
    # Initialisation du loader
    loader = Loader()
    # Importation des données
    df = loader.load(filepath=config['INPUT_DATA'])

    # Initialisation du builder
    builder = DuckdbTablesBuilder(df=df, categorical_threshold=config['THRESHOLD'], path=config['OUTPUT_DATA'])
    # Construction du schéma duckDB
    builder.build_duckdb_schema()
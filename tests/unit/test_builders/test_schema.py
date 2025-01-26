# Importation des modules
# Modules de base
import pandas as pd
import numpy as np
# Module de tests
import pytest
# Modules du package à tester
from dashboard_template_database.builders import SchemaBuilder

# Initialisation d'une instance de la classe utilisée dans l'ensemble des tests
@pytest.fixture
def schema_builder(sample_df):
    """Initialization of the SchemaBuilder class."""
    return SchemaBuilder(sample_df, categorical_threshold=4)

# Fonction de test de l'initialisation de la classe
def test_schema_builder_initialization(schema_builder, sample_df):
    """Test the initialization of the SchemaBuilder class."""
    # Vérification de son type et de la bonne initialisation des attributs
    assert isinstance(schema_builder, SchemaBuilder)
    assert schema_builder.df.equals(sample_df)
    assert schema_builder.categorical_threshold == 4

# Fonction de test de l'association des types en python et en SQL
def test_map_python_to_sql_type():
    """Test the mapping between python and SQL types."""
    # Vérification du mapping de chaque type
    assert SchemaBuilder._map_python_to_sql_type('object') == 'VARCHAR'
    assert SchemaBuilder._map_python_to_sql_type('int64') == 'INTEGER'
    assert SchemaBuilder._map_python_to_sql_type('float64') == 'DOUBLE'
    assert SchemaBuilder._map_python_to_sql_type('datetime64[ns]') == 'TIMESTAMP'
    assert SchemaBuilder._map_python_to_sql_type('bool') == 'BOOLEAN'
    assert SchemaBuilder._map_python_to_sql_type('unknown_type') == 'VARCHAR'

# Fonction de test de la création de la table des méta-données
def test_create_metadata_table(schema_builder):
    """Test the build of the metadata table."""
    # Création de la table des méta-données
    metadata = schema_builder.create_metadata_table()
    
    # Vérification du type renvoyé
    assert isinstance(metadata, pd.DataFrame)
    # Vérification de l'existence de chacune des colonnes
    assert 'name' in metadata.columns
    assert 'label' in metadata.columns
    assert 'python_type' in metadata.columns
    assert 'sql_type' in metadata.columns
    assert 'is_categorical' in metadata.columns
    
    # Vérification de la bonne détection des variables catégorielles
    assert metadata.loc[metadata['name'] == 'category', 'is_categorical'].iloc[0]
    assert metadata.loc[metadata['name'] == 'status', 'is_categorical'].iloc[0]
    assert not metadata.loc[metadata['name'] == 'high_cardinality', 'is_categorical'].iloc[0]

# Fonction de test de l'ajout de labels pour les colonnes lors de la création de la table des méta-données
def test_create_metadata_table_with_labels(schema_builder, column_labels):
    """Test the build of the metadata table with labels."""
    # Création de la table des méta-données avec les labels
    metadata = schema_builder.create_metadata_table(column_labels)
    
    # Vérification de la bonne association des labels
    for col, label in column_labels.items():
        assert metadata.loc[metadata['name'] == col, 'label'].iloc[0] == label

# Fonction de test de la création des tables de dimension
def test_create_dimension_tables(schema_builder):
    """Test the build of the dimension tables."""
    # Création de la table des méta-données
    schema_builder.create_metadata_table()
    # Création des tables de dimension
    dim_tables = schema_builder.create_dimension_tables()
    
    # Véirfication du type renvoyé et des éléments pris en compte
    assert isinstance(dim_tables, dict)
    assert 'category' in dim_tables
    assert 'status' in dim_tables
    
    # Vérification de la structure des tables de dimension
    for table in dim_tables.values():
        # Type
        assert isinstance(table, pd.DataFrame)
        # Colonnes
        assert 'value' in table.columns
        assert 'label' in table.columns

# Fonction de test de la création de la table des faits
def test_create_fact_table(schema_builder, sample_df):
    """Test the build of the fact table."""
    # Création de la table des méta-données
    schema_builder.create_metadata_table()
    # Création des tables de dimension
    schema_builder.create_dimension_tables()
    # Création de la table des faits
    fact_table = schema_builder.create_fact_table()
    
    # Vérification du type et de la longueur des tables respectives
    assert isinstance(fact_table, pd.DataFrame)
    assert fact_table.shape[0] == sample_df.shape[0]
    
    # Véirification que les variables catégorielles ont bien été remplacées par leurs identifiants
    assert fact_table['category'].dtype in [np.int64, np.int32]
    assert fact_table['status'].dtype in [np.int64, np.int32]

# Fonction de test de la construction complète du schéma
def test_build_complete_schema(schema_builder, column_labels):
    """Test the build of the complete scheme."""
    # Construction du schéma
    metadata, dim_tables, fact_table = schema_builder.build(column_labels)
    
    # Vérification du type de chacun des éléments du schéma
    assert isinstance(metadata, pd.DataFrame)
    assert isinstance(dim_tables, dict)
    assert isinstance(fact_table, pd.DataFrame)
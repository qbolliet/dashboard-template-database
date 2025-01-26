# Importation des modules
# DuckDB
import duckdb
# Module de tests
import pytest
# Module à tester
from dashboard_template_database.builders import DuckdbTablesBuilder


# Initialisation d'une instance de la classe utilisée dans l'ensemble des tests
@pytest.fixture
def duckdb_builder(sample_df):
    """Initialization of the DuckdbTablesBuilder class."""
    return DuckdbTablesBuilder(sample_df)

# Test de l'initialisation du constructeur
def test_duckdb_builder_initialization(sample_df):
    """Test the initialization of the DuckdbTablesBuilder class."""
    # Vérification de la connexion en mémoire
    builder = DuckdbTablesBuilder(sample_df)
    assert isinstance(builder.conn, duckdb.DuckDBPyConnection)
    
    # Vérification de la connexion à un fichier
    builder = DuckdbTablesBuilder(sample_df, path=':memory:')
    assert isinstance(builder.conn, duckdb.DuckDBPyConnection)

# Test de la création de la table des méta-données
def test_create_duckdb_metadata_table(duckdb_builder):
    """Test the build of the metadata table."""
    # Création de la table des méta-données
    duckdb_builder.create_duckdb_metadata_table(table_name='test_metadata')
    
    # Vérification que la table existe et a la strcuture attendue
    result = duckdb_builder.conn.execute("SELECT * FROM test_metadata").fetchdf()
    assert 'name' in result.columns
    assert 'label' in result.columns
    assert 'python_type' in result.columns
    assert 'sql_type' in result.columns
    assert 'is_categorical' in result.columns

# Test de la création des tables de dimensions
def test_create_duckdb_dimension_tables(duckdb_builder):
    """Test the build of the dimension tables."""
    # Création des tables de dimensions
    duckdb_builder.create_duckdb_dimension_tables(table_prefix='test_dim_')
    
    # Vérification que les tables existent
    tables = duckdb_builder.conn.execute("SHOW TABLES").fetchall()
    table_names = [t[0] for t in tables]
    
    assert 'test_dim_category' in table_names
    assert 'test_dim_status' in table_names
    
    # Vérification de la structure des tables de dimension
    for table in ['category', 'status']:
        result = duckdb_builder.conn.execute(f"SELECT * FROM test_dim_{table}").fetchdf()
        assert 'value' in result.columns
        assert 'label' in result.columns

# Test de la création de la table des faits
def test_create_duckdb_fact_table(duckdb_builder):
    """Test the build of the fact table."""
    # Création des tables nécessaires
    # Création de la table des méta-données
    duckdb_builder.create_duckdb_metadata_table()
    # Création des tables de dimension
    duckdb_builder.create_duckdb_dimension_tables()
    # Création de la table des faits
    duckdb_builder.create_duckdb_fact_table(table_name='test_fact')
    
    # Vérification que la table des faits existe, a la structure et les dimensions attendues
    result = duckdb_builder.conn.execute("SELECT * FROM test_fact").fetchdf()
    assert result.shape[0] == duckdb_builder.df.shape[0]
    assert 'category' in result.columns
    assert 'status' in result.columns
    
    # Vérification des clés étrangères
    dim_category = duckdb_builder.conn.execute("SELECT * FROM dim_category").fetchdf()
    dim_status = duckdb_builder.conn.execute("SELECT * FROM dim_status").fetchdf()
    
    assert set(result['category'].unique()) <= set(dim_category['value'])
    assert set(result['status'].unique()) <= set(dim_status['value'])

# Test de la création de l'ensemble du schéma
def test_build_duckdb_schema(duckdb_builder):
    """Test the build of the complete scheme."""
    # Création du schéma
    duckdb_builder.build_duckdb_schema(
        metadata_table='test_metadata',
        fact_table='test_fact',
        dim_table_prefix='test_dim_'
    )
    
    # Vérification de l'existence de l'ensemble des tables attendues
    tables = duckdb_builder.conn.execute("SHOW TABLES").fetchall()
    table_names = [t[0] for t in tables]
    
    assert 'test_metadata' in table_names
    assert 'test_fact' in table_names
    assert 'test_dim_category' in table_names
    assert 'test_dim_status' in table_names

# Test de l'affichage du schéma
def test_display_schema(duckdb_builder, caplog):
    """Test the display of the built scheme."""
    # Création du schéma
    duckdb_builder.build_duckdb_schema()
    # Affichage du schéma
    duckdb_builder.display_schema()
    
    # Vérification que l'information est bien rendue
    assert "Created Tables:" in caplog.text
    assert "Structure:" in caplog.text

# Vérification de la génération d'erreur pour des tables absentes
@pytest.mark.parametrize("table_name", ['invalid_table', 'nonexistent'])
def test_query_nonexistent_table(duckdb_builder, table_name):
    """Test error raised for non-existent tables."""
    with pytest.raises(duckdb.Error):
        duckdb_builder.conn.execute(f"SELECT * FROM {table_name}")
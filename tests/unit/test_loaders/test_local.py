# Importation des modules
# Modules de base
import pandas as pd
# Module de test
import pytest
# Module à tester
from dashboard_template_database.loaders.local import Loader

# Initialisation des fichiers de tests de différents formats
@pytest.fixture
def temp_files(sample_df, tmp_path):
    """Initialization of the files to load."""
    # Création de fichiers temporaires de différents formats
    files = {}
    
    # CSV
    csv_path = tmp_path / "test.csv"
    sample_df.to_csv(csv_path, index=False)
    files['csv'] = csv_path
    
    # Excel
    xlsx_path = tmp_path / "test.xlsx"
    sample_df.to_excel(xlsx_path, index=False, engine='openpyxl')
    files['xlsx'] = xlsx_path
    
    # Pickle
    pkl_path = tmp_path / "test.pkl"
    sample_df.to_pickle(pkl_path)
    files['pkl'] = pkl_path
    
    # Parquet
    parquet_path = tmp_path / "test.parquet"
    sample_df.to_parquet(parquet_path)
    files['parquet'] = parquet_path
    
    return files

# Initialisation d'une instance de la classe utilisée dans l'ensemble des tests
@pytest.fixture
def loader():
    """Initialization of the Loader class."""
    return Loader()

# Test de l'initialisation de la classe
def test_loader_initialization(loader):
    """Test the initialization of the Loader class."""
    assert isinstance(loader, Loader)

# Test du chargement de fichiers CSV
def test_load_csv(loader, temp_files, sample_df):
    """Test the loading of CSV files"""
    df = loader.load(str(temp_files['csv']))
    pd.testing.assert_frame_equal(df, sample_df)

# Test du chargement de fichiers Excel
def test_load_xlsx(loader, temp_files, sample_df):
    """Test the loading of Excel files"""
    df = loader.load(str(temp_files['xlsx']))
    pd.testing.assert_frame_equal(df, sample_df)

# Test du chargement de fichiers pickle
def test_load_pickle(loader, temp_files, sample_df):
    """Test the loading of pickle files"""
    df = loader.load(str(temp_files['pkl']))
    pd.testing.assert_frame_equal(df, sample_df)

# Test du chargement de fichiers parquet
def test_load_parquet(loader, temp_files, sample_df):
    """Test the loading of parquet files"""
    df = loader.load(str(temp_files['parquet']))
    pd.testing.assert_frame_equal(df, sample_df)

# Test de l'erreur pour les extensions non supportées
def test_invalid_extension(loader):
    """Test the 'invalid extension' error"""
    with pytest.raises(ValueError, match="Invalid extension"):
        loader.load("invalid.txt")

# Test du chargement de fichiers CSV avec des kwargs
def test_load_with_kwargs(loader, temp_files, sample_df):
    """Test the loading of CSV files with kwargs"""
    # Test with CSV using specific encoding
    df = loader.load(str(temp_files['csv']), encoding='utf-8')
    pd.testing.assert_frame_equal(df, sample_df)
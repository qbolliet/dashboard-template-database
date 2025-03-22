# Importation des modules
# Modules de base
import pandas as pd
import boto3
#from moto import mock_aws
# Modules de test
import pytest
# Modules à tester
from dashboard_template_database.storage import Loader

# Classe de test du Loader
class TestLoader:
    """Test suite for Loader class."""

    # Test de l'initialisation avec l'un des deux packages
    def test_initialization(self):
        """Test loader initialization."""
        # Initialisation avec boto3 (package par défaut)
        loader = Loader()
        assert loader.s3_package == 'boto3'  # Default package
        
        # Initialisation avec s3fs
        loader = Loader(s3_package='s3fs')
        assert loader.s3_package == 's3fs'

    # Test de la méthode de connexion pour S3
    # @mock_aws
    # def test_connect_method(self, aws_credentials):
    #     """Test connect method for S3."""
    #     # Initialisation du loader
    #     loader = Loader()
    #     # Connexion au bucket en passant les paramètres S3
    #     loader.load('test.csv', 'test-bucket', 
    #                 aws_access_key_id='testing',
    #                 aws_secret_access_key='testing')
    #     # Vérification de l'élément de connexion
    #     assert hasattr(loader, 's3')

    # Test de chargement de fichiers locaux de différents formats
    @pytest.mark.parametrize('file_format', ['csv', 'parquet', 'xlsx', 'pkl'])
    def test_load_local_different_formats(self, temp_files, sample_df, file_format):
        """Test Loader load method with different local file formats"""
        # Initialisation du loader
        loader = Loader()
        
        # Chargement du jeu de données local
        data = loader.load(
            filepath=str(temp_files[file_format])
        )
        # Changement du type en date
        data['date'] = pd.to_datetime(data['date'])
        
        # Vérification que les deux jeux de données correspondent
        pd.testing.assert_frame_equal(
            data,
            sample_df,
            check_dtype=False  # Some datatypes might change during save/load
        )

    # Test de l'erreur pour les extensions non supportées
    def test_invalid_extension(self):
        """Test the 'invalid extension' error"""
        loader = Loader()
        with pytest.raises(ValueError, match="Invalid extension"):
            loader.load("invalid.txt")

    # Test du chargement de fichiers CSV avec des kwargs
    def test_load_with_kwargs(self, temp_files, sample_df):
        """Test the loading of CSV files with kwargs"""
        # Initialisation du loader
        loader = Loader()
        
        # Test with CSV using specific encoding
        df = loader.load(str(temp_files['csv']), encoding='utf-8', parse_dates=[3])
        pd.testing.assert_frame_equal(df, sample_df, check_dtype=False)

# @mock_aws
# def test_load_nonexistent_file_s3(aws_credentials, setup_test_bucket):
#     """Test loading non-existent file from S3."""
#     # Initialisation du loader
#     loader = Loader(s3_package="boto3")
    
#     # Génère une exception avec boto3
#     with pytest.raises(Exception): 
#         loader.load(
#             filepath='nonexistent.csv',
#             bucket=setup_test_bucket,
#             aws_access_key_id="testing",
#             aws_secret_access_key="testing"
#         )


# @mock_aws
# def test_load_invalid_extension_s3(aws_credentials, setup_test_bucket):
#     """Test loading file with invalid extension from S3."""
#     # Create a test file with invalid extension
#     s3_client = boto3.client('s3', region_name='us-east-1')
#     s3_client.put_object(
#         Bucket=setup_test_bucket,
#         Key="test.invalid",
#         Body=b"test data"
#     )
    
#     # Initialisation du loader
#     loader = Loader(s3_package="boto3")
    
#     # Vérification du message d'erreur
#     with pytest.raises(ValueError, match="Invalid extension"):
#         loader.load(
#             filepath='test.invalid',
#             bucket=setup_test_bucket,
#             aws_access_key_id="testing",
#             aws_secret_access_key="testing"
#         )


# @pytest.mark.parametrize('file_format', ['csv', 'parquet', 'xlsx'])
# @mock_aws
# def test_load_different_formats_boto3(aws_credentials, setup_test_bucket, sample_df, file_format):
#     """Test S3 Loader load method with different file formats using boto3."""
#     # Initialisation du loader
#     loader = Loader(s3_package="boto3")
    
#     # Chargement du jeu de données
#     data = loader.load(
#         filepath=f"test.{file_format}",
#         bucket=setup_test_bucket,
#         aws_access_key_id="testing",
#         aws_secret_access_key="testing"
#     )
    
#     # Vérification que les deux jeux de données correspondent
#     pd.testing.assert_frame_equal(
#         data,
#         sample_df,
#         check_dtype=False  # Some datatypes might change during save/load
#     )
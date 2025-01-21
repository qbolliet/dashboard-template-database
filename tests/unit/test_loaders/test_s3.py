# Importation des modules
# Modules de base
import os
import pandas as pd
# Gestion de la connexion à S3
import boto3
import s3fs
from moto import mock_s3
# Module de tests
import pytest
# Module à tester
from dashboard_template_database.loaders.s3 import S3Loader
from dashboard_template_database.loaders.s3._connection import _S3Connection


# Classe de test de la connection au bucket S3
class TestS3Connection:
    """Test suite for _S3Connection class."""
    # Test de l'erreur lors d'une tentative de connexion avec un package non-valide
    def test_init_invalid_package(self):
        """Test initialization with invalid package."""
        with pytest.raises(ValueError, match="'package' must be in"):
            _S3Connection(package='invalid')

    # Test de l'initialisation avec l'un des deux packages valides
    def test_init_valid_package(self):
        """Test initialization with valid packages."""
        # Test de la connexion avec boto3
        conn_boto3 = _S3Connection(package='boto3')
        assert conn_boto3.package == 'boto3'
        
        # Test de la connexion avec s3fs
        conn_s3fs = _S3Connection(package='s3fs')
        assert conn_s3fs.package == 's3fs'

    # Test de la connexion avec boto3
    @mock_s3
    def test_connect_boto3(self, aws_credentials):
        """Test connection with boto3."""
        conn = _S3Connection(package='boto3')
        conn._connect()
        assert hasattr(conn, 's3')
        assert isinstance(conn.s3, boto3.client.BaseClient)

    # Test de la connexion avec s3fs
    def test_connect_s3fs(self, aws_credentials):
        """Test connection with s3fs."""
        conn = _S3Connection(package='s3fs')
        conn._connect()
        assert hasattr(conn, 's3')
        assert isinstance(conn.s3, s3fs.core.S3FileSystem)


# Classe de test du S3 loader
class TestS3Loader:
    """Test suite for S3Loader class."""

    # Test de l'initialisation avec l'un des deux packages
    def test_initialization(self):
        """Test loader initialization."""
        # Initialisation avec boto3 (package par défaut)
        loader = S3Loader()
        assert loader.package == 'boto3'  # Default package
        
        # Initialisation avec s3fs
        loader = S3Loader(package='s3fs')
        assert loader.package == 's3fs'

    # Test de la méthode de connexion
    @mock_s3
    def test_connect_method(self, aws_credentials):
        """Test connect method."""
        # Initialisation du loader
        loader = S3Loader()
        # Connexion au bucket
        loader.connect()
        # Vérification de l'élément du connexion
        assert hasattr(loader, 's3')

    # Test du chargement de fichiers de différents formats avec boto3
    @pytest.mark.parametrize('file_format', ['csv', 'parquet', 'xlsx'])
    @mock_s3
    def test_load_different_formats_boto3(self, s3_bucket_with_files, sample_df, file_format):
        """Test loading different file formats using boto3."""
        # Extraction du bucket
        bucket, _ = s3_bucket_with_files
        
        # Initialisation du loader
        loader = S3Loader(package='boto3')
        # Connexion au bucket
        loader.connect()
        
        # Chargement du fichier test
        result = loader.load(bucket, f'test.{file_format}')
        # Vérification du bon chargement des données
        pd.testing.assert_frame_equal(result, sample_df)

    # Test du chargement de fichiers de différents formats avec s3fs
    @pytest.mark.parametrize('file_format', ['csv', 'parquet', 'xlsx'])
    @mock_s3
    def test_load_different_formats_s3fs(self, s3_bucket_with_files, sample_df, file_format):
        """Test loading different file formats using s3fs."""
        # Extraction du bucket
        bucket, _ = s3_bucket_with_files
        
        # Initialisation du loader
        loader = S3Loader(package='s3fs')
        # Connexion au bucket
        loader.connect()
        
        # Chargement du fichier test
        result = loader.load(bucket, f'test.{file_format}')
        # Vérification du bon chargement des données
        pd.testing.assert_frame_equal(result, sample_df)

    # Test du chargement de fichiers de différents formats avec boto3
    @mock_s3
    def test_load_invalid_extension(self, s3_bucket_with_files):
        """Test loading file with invalid extension."""
        # Extraction du bucket
        bucket, _ = s3_bucket_with_files
        
        # Initialisation du loader
        loader = S3Loader()
        # Connexion au bucket
        loader.connect()
        
        # Vérification du message d'erreur
        with pytest.raises(ValueError, match="Invalid extension"):
            loader.load(bucket, 'test.invalid')

    # Test de l'erreur lors d'une tentative de chargement de fichiers inexistants
    @mock_s3
    def test_load_nonexistent_file(self, s3_bucket_with_files):
        """Test loading non-existent file."""
        # Extraction du bucket
        bucket, _ = s3_bucket_with_files
        
        # Initialisation du loader
        loader = S3Loader()
        # Connexion au bucket
        loader.connect()
        
        # Génre une ClientError avec boto3 et une FileNotFoundError avec s3fs
        with pytest.raises(Exception): 
            loader.load(bucket, 'nonexistent.csv')

    # Test du chargement avec des kwargs
    @mock_s3
    def test_load_with_kwargs(self, s3_bucket_with_files, sample_df):
        """Test loading with additional kwargs."""
        # Extraction du bucket
        bucket, _ = s3_bucket_with_files
        # Initialisation du loader
        loader = S3Loader()
        # Connexion au bucket
        loader.connect()
        
        # Test du chargement d'un fichier CSV avec un encoding spécifique
        result = loader.load(bucket, 'test.csv', encoding='utf-8')
        # Vérification du chargement
        pd.testing.assert_frame_equal(result, sample_df)


# Classe de test des cas d'erreur du S3Loader
class TestS3LoaderErrors:
    """Test suite for S3Loader error cases."""
    # Test de l'erreur du chargement des données sans réaliser avoir établi de connexion
    def test_load_without_connection(self, s3_bucket):
        """Test loading without establishing connection first."""
        # Initialisation du loader
        loader = S3Loader()
        # Une connexion doit automatiquement être créée s'il n'y en a pas déjà une
        loader.load(s3_bucket, 'test.csv')
        # Vérification de la présence de l'attribut de chargement
        assert hasattr(loader, 's3')
    
    # Test de l'erreur de credentials invalides
    @mock_s3
    def test_invalid_credentials(self):
        """Test connecting with invalid credentials."""
        # Initialisation de credentials invalides
        os.environ['AWS_ACCESS_KEY_ID'] = 'invalid'
        os.environ['AWS_SECRET_ACCESS_KEY'] = 'invalid'
        # Initialisation de la classe
        loader = S3Loader()
        # Test de l'erreur (le message d'erreur dépend du package utilisé)
        with pytest.raises(Exception): 
            loader.connect()
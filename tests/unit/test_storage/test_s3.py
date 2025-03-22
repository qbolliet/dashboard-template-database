# Importation des modules
# Modules de base
import s3fs
#from moto import mock_aws
# Modules de test
import pytest

# Modules à tester
from dashboard_template_database.storage.s3._connection import _S3Connection

# Classe de test de la connection au bucket S3
class TestS3Connection:
    """Test suite for _S3Connection class."""
    # Test de l'erreur lors d'une tentative de connexion avec un package non-valide
    def test_init_invalid_package(self):
        """Test initialization with invalid package."""
        with pytest.raises(ValueError, match="'s3_package' must be in"):
            _S3Connection(s3_package='invalid')

    # Test de l'initialisation avec l'un des deux packages valides
    def test_init_valid_package(self):
        """Test initialization with valid packages."""
        # Test de la connexion avec boto3
        conn_boto3 = _S3Connection(s3_package='boto3')
        assert conn_boto3.s3_package == 'boto3'
        
        # Test de la connexion avec s3fs
        conn_s3fs = _S3Connection(s3_package='s3fs')
        assert conn_s3fs.s3_package == 's3fs'

    # Test de la connexion avec boto3
    # @mock_aws
    # def test_connect_boto3(self, aws_credentials):
    #     """Test connection with boto3."""
    #     conn = _S3Connection(s3_package='boto3')
    #     conn._connect()
    #     assert hasattr(conn, 's3')

    # Test de la connexion avec s3fs
    def test_connect_s3fs(self, aws_credentials):
        """Test connection with s3fs."""
        conn = _S3Connection(s3_package='s3fs')
        conn._connect()
        assert hasattr(conn, 's3')
        assert isinstance(conn.s3, s3fs.core.S3FileSystem)

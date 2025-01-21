# Importation des modules
# Modules de base
import os
import numpy as np
import pandas as pd
# Module de connexion à S3
import boto3
from moto import mock_s3
# Module de tests
import pytest

# Initialisation d'un jeu de données d'exemple
@pytest.fixture
def sample_df():
    """Create a sample DataFrame for testing."""
    return pd.DataFrame({
        'id': range(1, 6),
        'category': ['A', 'B', 'A', 'C', 'B'],
        'value': np.random.rand(5),
        'date': pd.date_range('2024-01-01', periods=5),
        'status': ['active', 'inactive', 'active', 'active', 'inactive'],
        'high_cardinality': [f'val_{i}' for i in range(100, 105)]
    })

# Initialisation du dictionnaire de labels pour les colonnes
@pytest.fixture
def column_labels():
    """Fixture for DataFrame column labels."""
    return {
        'id': 'Identifier',
        'category': 'Category Name',
        'value': 'Numeric Value',
        'date': 'Date Field',
        'status': 'Status Field'
    }

# Initialisation des credentials de connexion
@pytest.fixture
def aws_credentials():
    """Mock AWS credentials for testing."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'
    os.environ['AWS_S3_ENDPOINT'] = 'test.endpoint'

# Initialisation du nom du bucket
@pytest.fixture
def s3_bucket():
    """Fixture for S3 bucket name."""
    return 'test-bucket'

# Initialisation d'une connexion s3
@pytest.fixture
def s3_client(aws_credentials):
    """Create mocked S3 client."""
    with mock_s3():
        conn = boto3.client('s3', region_name='us-east-1')
        yield conn

# Initialisation d'un bucket avec des fichiers
@pytest.fixture
def s3_bucket_with_files(s3_client, s3_bucket, sample_df):
    """Create a mocked S3 bucket with test files."""
    # Création d'un bucket
    s3_client.create_bucket(Bucket=s3_bucket)
    
    # Création de fichiers de différents formats
    files = {
        'test.csv': sample_df.to_csv(index=False).encode(),
        'test.parquet': sample_df.to_parquet(),
        'test.xlsx': sample_df.to_excel(None, index=False).getvalue(),
    }
    
    # Téléversement des fichiers sur bucket le S3
    for key, data in files.items():
        s3_client.put_object(Bucket=s3_bucket, Key=key, Body=data)
    
    return s3_bucket, files
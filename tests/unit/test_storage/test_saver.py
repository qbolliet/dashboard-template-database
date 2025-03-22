# Importation des modules
# Modules de base
import pandas as pd
# Gestion de la connexion à S3
import boto3
import s3fs
#from moto import mock_aws
# Module de tests
import pytest
# Module à tester - Updated imports
from dashboard_template_database.storage import Loader, Saver





# Classe de test du Saver
class TestSaver:
    """Test suite for Saver class."""

    # Test de l'initialisation avec l'un des deux packages
    def test_initialization(self):
        """Test saver initialization."""
        # Initialisation avec boto3 (package par défaut)
        saver = Saver()
        assert saver.s3_package == 'boto3'  # Default package
        
        # Initialisation avec s3fs
        saver = Saver(s3_package='s3fs')
        assert saver.s3_package == 's3fs'

    # Test de sauvegarde locale avec différents formats
    @pytest.mark.parametrize('file_format', ['csv', 'parquet', 'xlsx'])
    def test_save_local_different_formats(self, sample_df, tmp_path, file_format):
        """Test Saver save method with different local file formats"""
        # Initialisation du saver
        saver = Saver()
        
        # Chemin de sauvegarde
        save_path = tmp_path / f"save_test.{file_format}"
        
        # Sauvegarde du jeu de données
        saver.save(
            filepath=str(save_path),
            obj=sample_df,
            index=False
        )
        
        # Vérification que le fichier existe
        assert save_path.exists()
        
        # Chargement du fichier sauvegardé pour vérification
        loader = Loader()
        loaded_df = loader.load(str(save_path))
        # Conversion de la date
        loaded_df['date'] = pd.to_datetime(loaded_df['date'])
        
        # Vérification que les données sont correctes
        pd.testing.assert_frame_equal(
            loaded_df,
            sample_df,
            check_dtype=False  # Some datatypes might change during save/load
        )

    # Test de sauvegarde avec plusieurs feuilles Excel
    def test_save_excel_with_sheets(self, sample_df, tmp_path):
        """Test saving Excel with multiple sheets."""
        # Initialisation du saver
        saver = Saver()
        
        # Création d'un dictionnaire de DataFrames
        df_dict = {
            'Sheet1': sample_df,
            'Sheet2': sample_df.head(3)
        }
        
        # Chemin de sauvegarde
        save_path = tmp_path / "multi_sheet.xlsx"
        
        # Sauvegarde du dictionnaire
        saver.save(
            filepath=str(save_path),
            obj=df_dict,
            index=False
        )
        
        # Vérification que le fichier existe
        assert save_path.exists()
        
        # Chargement du fichier pour vérifier les deux feuilles
        loader = Loader()
        sheet1 = loader.load(str(save_path), sheet_name='Sheet1')
        sheet2 = loader.load(str(save_path), sheet_name='Sheet2')
        
        # Vérification des données
        pd.testing.assert_frame_equal(sheet1, sample_df, check_dtype=False)
        pd.testing.assert_frame_equal(sheet2, sample_df.head(3), check_dtype=False)

    # Test de l'erreur pour les extensions non supportées
    def test_invalid_extension_save(self, sample_df, tmp_path):
        """Test the 'invalid extension' error when saving"""
        saver = Saver()
        with pytest.raises(ValueError):
            saver.save(str(tmp_path / "invalid.txt"), obj=sample_df)

    # Test de sauvegarde avec des kwargs
    def test_save_with_kwargs(self, sample_df, tmp_path):
        """Test saving with additional kwargs."""
        # Initialisation du saver
        saver = Saver()
        
        # Chemin de sauvegarde
        save_path = tmp_path / "test_kwargs.csv"
        
        # Sauvegarde avec des arguments supplémentaires
        saver.save(
            filepath=str(save_path),
            obj=sample_df,
            index=False,
            encoding='utf-8'
        )
        
        # Vérification que le fichier existe
        assert save_path.exists()
        
        # Chargement pour vérification
        loader = Loader()
        loaded_df = loader.load(str(save_path))
        # Conversion de la date
        loaded_df['date'] = pd.to_datetime(loaded_df['date'])
        
        # Vérification des données
        pd.testing.assert_frame_equal(loaded_df, sample_df, check_dtype=False)


# @pytest.mark.parametrize('file_format', ['csv', 'parquet', 'xlsx'])
# @mock_aws
# def test_save_different_formats_boto3(aws_credentials, setup_test_bucket, sample_df, file_format):
#     """Test S3 Saver save method with different file formats using boto3."""
#     # Initialisation du saver et loader
#     saver = Saver(s3_package="boto3")
#     loader = Loader(s3_package="boto3")
    
#     # Clé pour le nouveau fichier
#     new_key = f"save_test.{file_format}"
    
#     # Sauvegarde du jeu de données
#     saver.save(
#         filepath=new_key,
#         bucket=setup_test_bucket,
#         obj=sample_df,
#         aws_access_key_id="testing",
#         aws_secret_access_key="testing"
#     )
    
#     # Vérification avec boto3 client que le fichier existe
#     s3_client = boto3.client('s3', region_name='us-east-1')
#     response = s3_client.list_objects_v2(Bucket=setup_test_bucket)
#     assert any(item['Key'] == new_key for item in response.get('Contents', []))
    
#     # Chargement pour vérification
#     loaded_df = loader.load(
#         filepath=new_key,
#         bucket=setup_test_bucket,
#         aws_access_key_id="testing",
#         aws_secret_access_key="testing"
#     )
    
#     # Vérification des données
#     pd.testing.assert_frame_equal(loaded_df, sample_df, check_dtype=False)


# @mock_aws
# def test_save_excel_with_multiple_sheets_s3(aws_credentials, setup_test_bucket, sample_df):
#     """Test saving Excel with multiple sheets to S3."""
#     # Initialisation du saver et loader
#     saver = Saver(s3_package="boto3")
#     loader = Loader(s3_package="boto3")
    
#     # Création d'un dictionnaire de DataFrames
#     df_dict = {
#         'Sheet1': sample_df,
#         'Sheet2': sample_df.head(3)
#     }
    
#     # Clé pour le fichier
#     key = "multi_sheet.xlsx"
    
#     # Sauvegarde du dictionnaire
#     saver.save(
#         filepath=key,
#         bucket=setup_test_bucket,
#         obj=df_dict,
#         aws_access_key_id="testing",
#         aws_secret_access_key="testing"
#     )
    
#     # Chargement pour vérification
#     sheet1 = loader.load(
#         filepath=key,
#         bucket=setup_test_bucket,
#         sheet_name='Sheet1',
#         aws_access_key_id="testing",
#         aws_secret_access_key="testing"
#     )
    
#     sheet2 = loader.load(
#         filepath=key,
#         bucket=setup_test_bucket,
#         sheet_name='Sheet2',
#         aws_access_key_id="testing",
#         aws_secret_access_key="testing"
#     )
    
#     # Vérification des données
#     pd.testing.assert_frame_equal(sheet1, sample_df, check_dtype=False)
#     pd.testing.assert_frame_equal(sheet2, sample_df.head(3), check_dtype=False)

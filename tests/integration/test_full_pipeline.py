# Importation des modules
# Module de tests
import pytest
# Modules d'imitation d'un environnement S3
from moto import mock_aws
# Modules du package à tester
from dashboard_template_database.loaders.s3 import S3Loader
from dashboard_template_database.builders import DuckdbTablesBuilder

@pytest.mark.integration
class TestFullPipeline:
    """Integration tests for the full data pipeline."""

    @pytest.fixture
    def setup_pipeline(self, s3_bucket_with_files, sample_df):
        """Setup the complete pipeline with all components."""
        # Initialisation du bucket
        bucket, _ = s3_bucket_with_files
        
        # Initialisation du loader
        s3_loader = S3Loader()
        
        return {
            'bucket': bucket,
            'loader': s3_loader,
            'sample_df': sample_df
        }

    @mock_aws
    def test_full_pipeline_execution(self, setup_pipeline):
        """Test the entire pipeline from S3 loading to DuckDB table creation."""
        # Extraction des inputs de la pipeline
        pipeline = setup_pipeline
        
        # 1. Chargement des données depuis s3
        df = pipeline['loader'].load(pipeline['bucket'], 'test.csv')
        
        # 2. Création des tables DuckDB
        db_builder = DuckdbTablesBuilder(df)
        
        # 3. Construction du schéma
        db_builder.build_duckdb_schema(
            metadata_table='test_metadata',
            fact_table='test_fact',
            dim_table_prefix='test_dim_'
        )
        
        # 4. Vérification des résultats
        # Vérification que les données correspondent à celles initialement enregistrées
        fact_data = db_builder.conn.execute("SELECT * FROM test_fact").fetchdf()
        assert len(fact_data) == len(pipeline['sample_df'])
        
        # Vérification que des méta-données sont renseignées
        metadata = db_builder.conn.execute("SELECT * FROM test_metadata").fetchdf()
        assert len(metadata) > 0
        
        # Vérification de la structure du schéma
        tables = db_builder.conn.execute("SHOW TABLES").fetchdf()
        assert 'test_metadata' in tables['name'].values
        assert 'test_fact' in tables['name'].values

    @mock_aws
    def test_pipeline_with_different_file_formats(self, setup_pipeline):
        """Test pipeline with different input file formats."""
        # Extraction des inputs de la pipeline
        pipeline = setup_pipeline
        
        # Parcours des différents formats de fichiers
        for file_format in ['csv', 'parquet', 'xlsx']:
            # Chargement des données
            df = pipeline['loader'].load(pipeline['bucket'], f'test.{file_format}')
            
            # Création des tables DuckDB
            db_builder = DuckdbTablesBuilder(df)
            db_builder.build_duckdb_schema()
            
            # Vérifiation des données
            fact_data = db_builder.conn.execute("SELECT * FROM fact_table").fetchdf()
            assert len(fact_data) == len(pipeline['sample_df'])
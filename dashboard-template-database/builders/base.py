import pandas as pd
import duckdb
import numpy as np
from typing import List, Dict, Optional, Union
from datetime import datetime

# Classe de création d'une base de données DuckDB avec :
# - Une "Fact table" : Contenant les données
# - Une "Label table" : Contenant les labels associés à chaque indicateur
# - Une "Metadata table" : Contenant les caractéristiques des variables de la factable (type, modalités etc ...)
class SchemaBuilder:

    # Initialisation
    def __init__(self, df: pd.DataFrame, categorical_threshold: Optional[int] = 50):
        """
        Initialize the schema builder with a dataframe and configuration.
        
        Args:
            df: Input DataFrame
            categorical_threshold: Maximum number of unique values for a column to be considered categorical
        """
        # Initialisation des arguments
        # Jeu de données
        self.df = df
        # Initialisation du seuil au deçà duquel les modalités d'une variable catégorielle ne sont plus exportées dans 
        self.categorical_threshold = categorical_threshold
    
    # Méthode inférant le type des colonnes du jeu de données 
    def create_metadata_table(self, column_labels : Optional[Union[Dict[str, str], None]]= None) -> Dict:
        """Automatically infer column types and identify categorical variables."""
        # Initialisation de la liste des méta-données
        list_metadata = []
        # Parcours des colonnes du jeu de données
        for col in self.df.columns:
            # Extraction du type de la colonne
            dtype = str(self.df[col].dtype)
            # Initialisation des méta-données associées à la colonne
            if column_labels is not None :
                metadata = {
                    'name': col,
                    'label': column_labels[col] if col in column_labels.keys() else col.replace('_', ' ').title(),
                    'python_type': dtype,
                    'sql_type': self._map_python_to_sql_type(dtype),
                    'is_categorical': False,
                    'modalities': None
                }
            else :
                metadata = {
                    'name': col,
                    'label': col.replace('_', ' ').title(),
                    'python_type': dtype,
                    'sql_type': self._map_python_to_sql_type(dtype),
                    'is_categorical': False,
                    'modalities': None
                }
            
            # Si la colonne
            if dtype == object :
                # Si le nombre de modalités dans la colonne est inférieur au seuil, la variable est catégorielle
                if self.df[col].nunique() <= self.categorical_threshold:
                    # Mise à jour du type de la variable
                    metadata['is_categorical'] = True
                    # Mise à jour des modalités
                    metadata['modalities'] = str(self.df[col].dropna().unique().tolist())
            
            # Ajout au dictionnaire
            list_metadata.append(metadata)
        
        # Création d'un DataFrame
        df_metadata = pd.DataFrame.from_dict(list_metadata)
        
        return df_metadata

    # Correspondance entre les types "python" et "SQL"
    @staticmethod
    def _map_python_to_sql_type(dtype: str) -> str:
        """Map Python data types to SQL data types."""
        # /!\ Peut être mis dans un json de paramètres
        # Dictionnaire des correspondance entre les types Python et SQL
        type_mapping = {
            'object': 'VARCHAR',
            'int64': 'INTEGER',
            'float64': 'DOUBLE',
            'datetime64[ns]': 'TIMESTAMP',
            'bool': 'BOOLEAN'
        }
        return type_mapping.get(dtype, 'VARCHAR')
    
    # Méthode créant la dimension table
    def create_dimension_table(self) :

class DuckDBSchemaBuilder:

    # Initialisation
    def __init__(self, df: pd.DataFrame, db_path: str, categorical_threshold: Optional[int] = 50):
        """
        Initialize the schema builder with a dataframe and configuration.
        
        Args:
            df: Input DataFrame
            db_path: Path to save the DuckDB database
            categorical_threshold: Maximum number of unique values for a column to be considered categorical
        """
        # Initialisation des arguments
        # Jeu de données
        self.df = df
        # Initialisation de la connection à la base de données
        self.conn = duckdb.connect(db_path)
        # Initialisation du seuil au deçà duquel les modalités d'une variable catégorielle ne sont plus exportées dans 
        self.categorical_threshold = categorical_threshold
        
    

    # Méthode créant la dimension table
    def create_dimension_table(self) -> None:
        """Create dimension tables for categorical columns."""

        # Create country dimension table
        country_query = """
        CREATE TABLE dim_country AS
        SELECT ROW_NUMBER() OVER() as country_id,
               country as country_name
        FROM (SELECT DISTINCT country FROM df)
        """
        self.conn.execute(country_query)
        
        # Create indicator dimension table
        indicator_query = """
        CREATE TABLE dim_indicator AS
        SELECT ROW_NUMBER() OVER() as indicator_id,
               indicator as indicator_name,
               horizon as horizon_value
        FROM (SELECT DISTINCT indicator, horizon FROM df)
        """
        self.conn.execute(indicator_query)

    def create_fact_table(self):
        """Create the fact table with foreign keys to dimension tables."""
        fact_query = """
        CREATE TABLE fact_values AS
        SELECT 
            i.indicator_id,
            c.country_id,
            df.date,
            df.value
        FROM df
        JOIN dim_indicator i ON df.indicator = i.indicator_name 
        JOIN dim_country c ON df.country = c.country_name
        """
        self.conn.execute(fact_query)

    def create_metadata_table(self, metadata: Dict):
        """Create metadata table with column information."""
        metadata_records = []
        for col_name, meta in metadata.items():
            metadata_records.append({
                'column_name': meta['name'],
                'column_label': meta['label'],
                'data_type': meta['sql_type'],
                'is_categorical': meta['is_categorical'],
                'modalities': str(meta['modalities']) if meta['modalities'] else None
            })
            
        metadata_df = pd.DataFrame(metadata_records)
        self.conn.execute("CREATE TABLE metadata AS SELECT * FROM metadata_df")

    def build_schema(self):
        """Execute the full schema building process."""
        try:
            # Register the DataFrame with DuckDB
            self.conn.execute("CREATE TABLE df AS SELECT * FROM self.df")
            
            # Infer metadata
            metadata = self.infer_column_types()
            
            # Create all tables
            self.create_dimension_tables(metadata)
            self.create_fact_table()
            self.create_metadata_table(metadata)
            
            # Create indexes for better performance
            self.conn.execute("CREATE INDEX idx_fact_date ON fact_values(date)")
            self.conn.execute("CREATE INDEX idx_fact_indicator ON fact_values(indicator_id)")
            self.conn.execute("CREATE INDEX idx_fact_country ON fact_values(country_id)")
            
            return True
            
        except Exception as e:
            print(f"Error building schema: {str(e)}")
            return False
        
    def close(self):
        """Close the database connection."""
        self.conn.close()

def build_database(
    df: pd.DataFrame,
    db_path: str,
    categorical_threshold: int = 50
) -> bool:
    """
    Convenience function to build the complete database from a DataFrame.
    
    Args:
        df: Input DataFrame
        db_path: Path to save the DuckDB database
        categorical_threshold: Maximum number of unique values for a column to be considered categorical
    
    Returns:
        bool: True if successful, False otherwise
    """
    builder = DuckDBSchemaBuilder(df, db_path, categorical_threshold)
    success = builder.build_schema()
    builder.close()
    return success
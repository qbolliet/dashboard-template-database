# Importation des modules
# Modules de base
import os
import pandas as pd
from typing import Dict, Optional, Union
# Duckdb
import duckdb

# Modules ad hoc
from .schema import SchemaBuilder

# Classe créant les tables correspondant au schéma
class DuckdbTablesBuilder(SchemaBuilder):

    # Initialisation
    def __init__(self, df: pd.DataFrame, categorical_threshold: Optional[int] = 50, connection: Optional[duckdb.DuckDBPyConnection] = None, path : Optional[Union[os.PathLike, None]]=None):
        # Initialisation du schéma
        super().__init__(df=df, categorical_threshold=categorical_threshold)
        
        # Initialisation de la connection
        if (connection is None) & (path is None) :
            self.conn = duckdb.connect(':memory:')
        elif (connection is None) :
            self.conn = duckdb.connect(path)
        else :
            self.conn = connection
    
    # Méthode de création de la table des méta-données
    def create_duckdb_metadata_table(self, table_name: Optional[str] = 'metadata', column_labels: Optional[Dict[str, str]] = None) -> None:
        # Création de la table des méta-données si elle n'existe pas déjà
        if not hasattr(self, 'df_metadata'):
            _ = self.create_metadata_table(column_labels)
        
        # Conversion DataFrame en table DuckDB
        self.conn.register('temp_metadata', self.df_metadata)
        self.conn.execute(f"""
            CREATE TABLE {table_name} AS 
            SELECT * FROM temp_metadata
        """)
        self.conn.execute('DROP VIEW temp_metadata')
    
    # Méthode de création des tables de dimensions
    def create_duckdb_dimension_tables(self, table_prefix: Optional[str] = 'dim_', column_labels: Optional[Dict[str, str]] = None) -> None:
        # Création du dictionnaire des tables de dimensions si elles n'existent pas déjà
        if not hasattr(self, 'dimension_tables'):
            _ = self.create_dimension_tables(column_labels)
        
        # Création de chaque table de dimension dans DuckDB
        for dim_name, dim_df in self.dimension_tables.items():
            # Initialisation du nom de la table
            table_name = f"{table_prefix}{dim_name}"
            # Enregistrement d'une vue temporaire
            self.conn.register('temp_dim', dim_df)
            
            # Création d'une table avec "value" comme clé primaire
            self.conn.execute(f"""
                CREATE TABLE {table_name} AS 
                SELECT
                    value,
                    label 
                FROM temp_dim
            """)
            
            # Ajout de la vue correspondante
            self.conn.execute('DROP VIEW temp_dim')
    
    # Méthode de création de la table d'informations
    def create_duckdb_fact_table(self, table_name: Optional[str] = 'fact_table', table_prefix: Optional[str] = 'dim_', column_labels: Optional[Dict[str, str]] = None) -> None:
        # Création de la table d'informations si elle n'existe pas déjà
        if not hasattr(self, 'df_fact'):
            _ = self.create_fact_table(column_labels)
        
        # Enregistrement de la table comme vue temporaire
        self.conn.register('temp_fact', self.df_fact)
        
        # Initialisation de la liste des conditions de jointure avec les tables de dimensions
        join_conditions = []
        # Initialisation de la liste des colonnes à sélectionner
        # select_columns = []
        
        # Parcours des tables de dimensions correspondant à des clés étrangères dans la table d'informations
        for dim_name in self.dimension_tables.keys():
            # Initialisation du nom de la table
            dim_table = f"{table_prefix}{dim_name}"
            
            # Ajout des conditions de jointure sur la base de la colonne "value" des tables de dimensions
            join_conditions.append(
                f"LEFT JOIN {dim_table} ON temp_fact.{dim_name} = {dim_table}.value"
            )
            
            # Ajout de la colonne qui correspond à une clé étrangère
            # select_columns.append(f"temp_fact.{dim_name}")
        
        # Ajout des colonnes des variables non catégorielles
        # select_columns.extend([f"temp_fact.{col}" for col in self.df_fact.columns if col not in self.dimension_tables.keys()])
        
        # Création de la table d'information
        query = f"""
        CREATE TABLE {table_name} AS 
        SELECT *
        FROM temp_fact
        {' '.join(join_conditions)}
        """
        
        self.conn.execute(query)

        # Ajout de la vue
        self.conn.execute('DROP VIEW temp_fact')
    
    # Méthode de construction du schéma
    def build_duckdb_schema(self, metadata_table: Optional[str] = 'metadata', fact_table: Optional[str] = 'fact_table', dim_table_prefix: Optional[str] = 'dim_', column_labels: Optional[Dict[str, str]] = None) -> None:
        # Création de la table des méta-données
        self.create_duckdb_metadata_table(
            table_name=metadata_table, 
            column_labels=column_labels
        )
        
        # Création de la table de dimensions
        self.create_duckdb_dimension_tables(
            table_prefix=dim_table_prefix, 
            column_labels=column_labels
        )
        
        # Création de la table d'informations avec les clés étrangères
        self.create_duckdb_fact_table(
            table_name=fact_table, 
            table_prefix=dim_table_prefix, 
            column_labels=column_labels
        )
    
    # Méthode d'affichage du schéma
    def display_schema(self) -> None:
        # Extraction des tables
        tables = self.conn.execute("SHOW TABLES").fetchall()
        print("Created Tables:")
        # Parcours des tables
        for table in tables:
            # Affichage de la structure
            print(f"\n{table[0]} Structure:")
            # Extraction des informations relatives à la table
            table_info = self.conn.execute(f"DESCRIBE {table[0]}").fetchall()
            # Affichage de chaque information
            for col in table_info:
                print(f"  {col[0]}: {col[1]}")
# Importation des modules
# Modules de base
import pandas as pd
from typing import Dict, Optional, Tuple, Union

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
            
            # Si la colonne est object
            if dtype == 'object' :
                # Si le nombre de modalités dans la colonne est inférieur au seuil, la variable est catégorielle
                if self.df[col].nunique() <= self.categorical_threshold:
                    # Mise à jour du type de la variable
                    metadata['is_categorical'] = True
                    # Mise à jour des modalités
                    # metadata['modalities'] = str(self.df[col].dropna().unique().tolist())
            
            # Ajout au dictionnaire
            list_metadata.append(metadata)
        
        # Création d'un DataFrame
        self.df_metadata = pd.DataFrame.from_dict(list_metadata)
        
        return self.df_metadata

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
    def create_dimension_tables(self, column_labels : Optional[Union[Dict[str, str], None]]= None) -> Dict[str, pd.DataFrame] :
        # Création d'une table de méta-données si cette-dernière n'existe pas déjà
        if not hasattr(self, 'metadata') :
            _ = self.create_metadata_table(column_labels=column_labels)

        # Initialisation du dictionnaire des tables de dimension
        self.dimension_tables = {}

        # Parcours des tables de dimensions
        for categorical_dimension in self.df_metadata.loc[self.df_metadata['is_categorical'], 'name'] :
            # Extraction des modalités
            self.dimension_tables[categorical_dimension] = pd.Series(self.df[categorical_dimension].unique(), name='label').to_frame().reset_index(names='value')

        return self.dimension_tables

    # Méthode créant la table des informations
    def create_fact_table(self, column_labels : Optional[Union[Dict[str, str], None]]= None) -> pd.DataFrame:
        # Création des tables de dimensions si ces-dernières n'existent pas
        if not hasattr(self, 'dimension_tables') :
            _ = self.create_dimension_tables(column_labels=column_labels)
        
        # Initialisation de la table des informations
        self.df_fact = self.df.copy()

        # Remplacement des labels par leur valeur
        for column in self.dimension_tables.keys() :
            # Construction du dictionnaire de passage
            dict_label_value = {d['label'] : d['value'] for d in self.dimension_tables[column].to_dict(orient='records')}
            # Remplacement des valeurs
            self.df_fact[column] = self.df_fact[column].replace(dict_label_value)
        
        return self.df_fact
    
    # Méthode créant les différentes tables
    def build(self, column_labels : Optional[Union[Dict[str, str], None]]= None) -> Tuple[pd.DataFrame, Dict[str, pd.DataFrame], pd.DataFrame] :
        # Création de la table des méta-données
        _ = self.create_metadata_table(column_labels=column_labels)
        # Création des tables de dimension
        _ = self.create_dimension_tables(column_labels=column_labels)
        # Création des tables d'informations
        _ = self.create_fact_table(column_labels=column_labels)

        return self.df_metadata, self.dimension_tables, self.df_fact
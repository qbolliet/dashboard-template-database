# Importation des modules
# Modules de base
import os
import warnings
from typing import Any

import narwhals as nw
from narwhals.typing import IntoDataFrame

# Module d'initialisation du logger
from ..utils.logger import _init_logger

# Utilitaires de traitement des données
from ..utils.types import map_python_to_sql_type


# Classe de création d'une base de données DuckDB avec :
# - Une "Fact table" : contenant les données, libellés d'origine compris
# - Une "Metadata table" : contenant les caractéristiques des variables de la fact table
# (libellé, type SQL, statut catégoriel, clé primaire)
class SchemaBuilder:
    """
    A class to automate the inference of the metadata and fact tables from a given
    DataFrame (pandas, polars, or narwhals-compatible).

    Categorical columns keep their original labels in the fact table — there is no
    dimension table and no synthetic code anywhere in the schema.

    Attributes:
        df (nw.DataFrame): The input dataset (converted to narwhals).
        categorical_threshold (int): Threshold below which a textual column is
            flagged as categorical, a UI-only piece of metadata.
        categorical_overrides (dict[str, bool]): Per-column forcing of the
            categorical status, independent of the threshold.
        primary_keys (list[str]): Logical primary key columns.
        logger (logging.Logger): Logger instance for tracking processing steps.
    """

    # Initialisation
    def __init__(
        self,
        df: IntoDataFrame,
        categorical_threshold: int | None = None,
        primary_keys: list[str] | None = None,
        categorical_overrides: dict[str, bool] | None = None,
        log_filename: str | os.PathLike[str] | None = None,
    ) -> None:
        """
        Initialize the SchemaBuilder with a DataFrame and optional parameters.

        Args:
            df: The input dataset (pandas, polars, or narwhals-compatible).
            categorical_threshold (Optional[int]): Maximum number of unique values
                for a textual column to be flagged as categorical. When None
                (default), no column is inferred as categorical. The flag is pure UI
                metadata: it drives no storage decision, since the fact table always
                stores the original labels.
            primary_keys (List[str], optional): List of column names to use as primary
            key.
                Can be a single column or composite key. Defaults to None (no primary
                key).
                When None, deduplication will use all columns and a UserWarning is
                raised.
            categorical_overrides (Optional[Dict[str, bool]]): Per-column forcing of
                the categorical status, independent of the threshold. A forced column
                is marked ``is_categorical_forced`` in the metadata table and is never
                re-evaluated by a subsequent update. Defaults to None.
            log_filename (os.PathLike, optional): Path to the log file. Defaults to
                a file named `schema_builder.log` in a logs directory.

        Raises:
            ValueError: If a primary key or a ``categorical_overrides`` key does not
                exist in the DataFrame.

        Examples:
            >>> # Single primary key (with polars)
            >>> import polars as pl
            >>> df = pl.DataFrame({'id': [1, 2, 3], 'value': [10, 20, 30]})
            >>> builder = SchemaBuilder(df, primary_keys=['id'])

            >>> # Composite primary key
            >>> builder = SchemaBuilder(df, primary_keys=['date', 'country',
            'indicator'])

            >>> # No primary key — raises UserWarning
            >>> builder = SchemaBuilder(df)

            >>> # Threshold disabled — no column is inferred as categorical
            >>> builder = SchemaBuilder(df, categorical_threshold=None,
            primary_keys=['id'])

            >>> # A column forced as categorical whatever its cardinality
            >>> builder = SchemaBuilder(df, categorical_threshold=50,
            ...     primary_keys=['id'], categorical_overrides={'city': True})
        """
        # Conversion vers narwhals
        self.df = nw.from_native(df, eager_only=True)
        # Initialisation du seuil en deçà duquel une colonne textuelle est signalée
        # comme catégorielle dans la table de méta-données
        self.categorical_threshold = categorical_threshold

        # Validation du forçage du statut catégoriel si spécifié
        if categorical_overrides:
            # Vérification de l'existence des colonnes visées
            unknown_cols = set(categorical_overrides) - set(self.df.columns)
            if unknown_cols:
                raise ValueError(
                    f"The following categorical_overrides columns do not exist in the"
                    f" DataFrame: {sorted(unknown_cols)}"
                )
        self.categorical_overrides = categorical_overrides or {}

        # Validation des clés primaires si spécifiées
        if primary_keys is not None and len(primary_keys) > 0:
            # Vérification de l'existence des colonnes
            missing_cols = set(primary_keys) - set(self.df.columns)
            if missing_cols:
                raise ValueError(
                    f"The following primary key columns do not exist in the DataFrame:"
                    f"{missing_cols}"
                )

            self.primary_keys = primary_keys
        else:
            self.primary_keys = []

        # Avertissement en l'absence de clés primaires : la déduplication portera sur
        # l'ensemble des colonnes, ce qui peut produire des résultats inattendus lorsque
        # plusieurs colonnes de valeurs (value, lower_bound, upper_bound…) diffèrent
        # entre deux lignes qui représentent pourtant le même enregistrement.
        if not self.primary_keys:
            warnings.warn(
                "No primary key specified. The deduplication will apply to "
                "all columns. Pass primary_keys=['col1', ...] to "
                "avoid this warning.",
                UserWarning,
                stacklevel=2,
            )

        # Initialisation du logger nommé.
        # Chemin par défaut centralisé dans utils.logger : <cwd>/logs/<name>.log.
        self.logger = _init_logger(filename=log_filename, name="schema_builder")

        # Logging de la validation des clés primaires (après initialisation du logger)
        if len(self.primary_keys) > 0:
            self.logger.info(
                f"Primary keys validated successfully: {self.primary_keys}"
            )

    # Méthode inférant le type des colonnes du jeu de données
    def create_metadata_table(
        self, column_labels: dict[str, str] | None | None = None
    ) -> nw.DataFrame[Any]:
        """
        Automatically infer metadata for the DataFrame's columns, including SQL types,
        labels, and the categorical UI flag.

        The ``is_categorical`` flag is inferred from the cardinality of textual
        columns and can be forced per column through ``categorical_overrides``; a
        forced column carries ``is_categorical_forced = True`` so that no later
        update re-evaluates it.

        Args:
            column_labels (dict, optional): A dictionary mapping column names to labels.
                                            Defaults to None.

        Returns:
            nw.DataFrame: A DataFrame containing metadata for each column in the input
            dataset.

        Examples:
            >>> metadata = builder.create_metadata_table()
            >>> sorted(metadata.columns)  # doctest: +NORMALIZE_WHITESPACE
            ['is_categorical', 'is_categorical_forced', 'is_primary_key', 'label',
             'name', 'sql_type']
        """
        # Initialisation de la liste des méta-données
        list_metadata = []
        # Parcours des colonnes du jeu de données
        for col in self.df.columns:
            # Extraction du type de la colonne (narwhals DType)
            dtype_obj = self.df.schema[col]
            # Initialisation des méta-données associées à la colonne
            if column_labels is not None:
                metadata = {
                    "name": col,
                    "label": column_labels[col]
                    if col in column_labels.keys()
                    else col.replace("_", " ").title(),
                    "sql_type": map_python_to_sql_type(dtype_obj),
                    "is_categorical": False,
                    "is_categorical_forced": False,
                    "is_primary_key": col in self.primary_keys,
                }
            else:
                metadata = {
                    "name": col,
                    "label": col.replace("_", " ").title(),
                    "sql_type": map_python_to_sql_type(dtype_obj),
                    "is_categorical": False,
                    "is_categorical_forced": False,
                    "is_primary_key": col in self.primary_keys,
                }

            # Logging
            self.logger.info(f"Successfully extracted meta-data from column '{col}'")

            # Vérification si la colonne est de type String (équivalent narwhals de
            # 'object')
            if isinstance(dtype_obj, nw.String):
                # Calcul du nombre de modalités
                n_modalities = self.df[col].n_unique()
                # Vérification du seuil : si categorical_threshold vaut None, aucune
                # colonne
                # n'est traitée comme catégorielle, quel que soit son nombre de
                # modalités.
                if self.categorical_threshold is not None:
                    if n_modalities <= self.categorical_threshold:
                        # Mise à jour du type de la variable
                        metadata["is_categorical"] = True
                        # Logging
                        self.logger.info(
                            f"The column '{col}' is of type 'String' and the number of"
                            f" modalities {n_modalities} satisfies the categorical"
                            f" threshold criteria {self.categorical_threshold}"
                        )
                        # Mise à jour des modalités
                        # metadata['modalities'] =
                        # str(self.df[col].dropna().unique().tolist())
                    else:
                        # Logging
                        self.logger.warning(
                            f"The column '{col}' is of type 'String' but the number of"
                            f" modalities {n_modalities} exceeds the categorical"
                            f" threshold criteria {self.categorical_threshold}"
                        )

            # Forçage explicite du statut catégoriel, indépendant du seuil.
            # Le marqueur is_categorical_forced empêche toute ré-évaluation ultérieure
            # par un update.
            if col in self.categorical_overrides:
                metadata["is_categorical"] = self.categorical_overrides[col]
                metadata["is_categorical_forced"] = True
                # Logging
                self.logger.info(
                    f"The categorical status of column '{col}' is forced to"
                    f" {metadata['is_categorical']} by categorical_overrides"
                )

            # Ajout au dictionnaire
            list_metadata.append(metadata)

        # Transformation de la liste de dicts en dict de listes (format attendu par
        # nw.from_dict)
        keys = list(list_metadata[0].keys())
        col_oriented = {k: [d[k] for d in list_metadata] for k in keys}
        # Création du DataFrame de métadonnées via narwhals (même backend que self.df)
        self.df_metadata = nw.from_dict(
            col_oriented, backend=nw.get_native_namespace(self.df)
        ).sort("label")

        # Logging
        self.logger.info("Successfully built the meta-data DataFrame")

        return self.df_metadata

    # Méthode créant la table des informations
    def create_fact_table(
        self, column_labels: dict[str, str] | None | None = None
    ) -> nw.DataFrame[Any]:
        """
        Return the fact table, i.e. the input dataset itself.

        Categorical columns keep their **original labels**: Parquet
        dictionary-encoding absorbs the storage cost, so no synthetic code and no
        dimension table are involved. The frame is copied rather than aliased so
        that later mutations of the fact table do not reach the input dataset.

        Args:
            column_labels (dict, optional): Accepted for signature parity with the
                other builders; unused here. Defaults to None.

        Returns:
            nw.DataFrame: The fact table, holding the input values verbatim.

        Examples:
            >>> fact_table = builder.create_fact_table()
            >>> fact_table['category'].to_list()
            ['A', 'B', 'A']
        """
        # Table des faits : copie du jeu de données d'entrée, sans substitution
        self.df_fact = self.df.clone()

        # Logging
        self.logger.info("Successfully built fact table")

        return self.df_fact

    # Méthode créant les différentes tables
    def build(
        self, column_labels: dict[str, str] | None | None = None
    ) -> tuple[nw.DataFrame[Any], nw.DataFrame[Any]]:
        """
        Execute the full pipeline to create the metadata and fact tables.

        Args:
            column_labels (dict, optional): A dictionary mapping column names to labels.
                                            Defaults to None.

        Returns:
            tuple: A tuple containing the metadata DataFrame and the fact table
            DataFrame, both narwhals frames.

        Examples:
            >>> metadata, fact_table = builder.build()
            >>> len(metadata) == len(fact_table.columns)
            True
        """
        # Création de la table des méta-données
        _ = self.create_metadata_table(column_labels=column_labels)
        # Création de la table des faits
        _ = self.create_fact_table(column_labels=column_labels)

        return self.df_metadata, self.df_fact

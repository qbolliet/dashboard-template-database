# Importation des modules
# Modules de base
import os
import threading
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any

# DuckDB
import duckdb
import narwhals as nw
import polars as pl
from narwhals.typing import IntoDataFrame

# Import des utilitaires
from ...utils.logger import _init_logger
from ...utils.sql import qualify_table, quote_ident, resolve_catalog
from ...utils.types import map_python_to_sql_type, resolve_sql_type_conflict


# Classe contenant des opérations utilitaires de base sur la base de données au schéma
# (table des faits - méta-données - méta-données du jeu de résultats)
class BaseSchemaManager(ABC):
    """
    Base class for database schema management operations.

    Provides common functionality for metadata management, column operations,
    and database introspection. All concrete managers should inherit from this class.

    Attributes:
        conn (duckdb.DuckDBPyConnection): Database connection
        categorical_threshold (int): Threshold for categorical determination
        schema (str): DuckLake schema holding this result set's tables
        catalog_alias (str): Alias of the attached DuckLake catalog (``ATTACH ...
            AS <alias>``), carried alongside ``schema`` so table references can be
            fully qualified by the catalog.
        logger: Logger instance for operation tracking
    """

    # Initialisation
    def __init__(
        self,
        connection: duckdb.DuckDBPyConnection | None = None,
        categorical_threshold: int | None = 50,
        log_filename: str | os.PathLike[str] | None = None,
        schema: str = "main",
        catalog_alias: str = "db",
    ):
        """
        Initialize the base schema manager.

        Args:
            connection: DuckDB connection attached to a DuckLake catalog, obtained
                via ``DuckLakeConnector.connect()``. If None, an in-memory DuckDB
                connection is created (useful for unit tests only).
            categorical_threshold: Threshold for determining categorical variables.
            log_filename: Path to log file.
            schema: DuckLake schema holding the ``fact_table``, ``metadata`` and
                ``dataset_metadata`` tables to operate on. A single catalog can host
                several schemas (one per result set). Defaults to ``'main'``.
            catalog_alias: Alias of the attached DuckLake catalog, matching the
                one passed to ``DuckLakeConnector`` (``ATTACH ... AS <alias>``).
                Carried alongside ``schema`` so that table references can be
                qualified by the catalog rather than resolved against the
                connection's current catalog. Defaults to ``'db'``.

        Example:
            >>> conn = DuckLakeConnector('catalog.ducklake', 'data/').connect()
            >>> manager = ConcreteManager(conn, categorical_threshold=30)
            >>> # Cibler un schéma dédié dans le même catalogue
            >>> manager = ConcreteManager(conn, schema='predictions')
        """
        # Initialisation de la connexion DuckLake.
        # Le fallback :memory: est réservé aux tests unitaires ; en production la
        # connexion doit toujours être fournie via DuckLakeConnector.connect().
        self.conn = connection if connection is not None else duckdb.connect(":memory:")

        # Seuil pour déterminer si une variable est catégorielle
        self.categorical_threshold = categorical_threshold

        # Schéma DuckLake cible : toutes les requêtes qualifient les tables par ce
        # schéma, permettant à plusieurs jeux de résultats de coexister dans un même
        # catalogue.
        self.schema = schema

        # Alias du catalogue DuckLake attaché : conservé au même titre que le schéma
        # afin de pouvoir qualifier les tables par le catalogue (indispensable dès
        # que plusieurs catalogues sont attachés à la même connexion).
        self.catalog_alias = catalog_alias

        # Alias de catalogue effectif : l'alias n'est utilisé pour qualifier les
        # tables que s'il correspond à une base réellement attachée. Les connexions
        # in-memory des tests n'attachent aucun catalogue : la qualification retombe
        # alors sur le seul schéma.
        self._catalog = resolve_catalog(self.conn, self.catalog_alias)

        # Initialisation du logger nommé pour traçabilité des opérations.
        # Chemin par défaut centralisé dans utils.logger : <cwd>/logs/<name>.log.
        self.logger = _init_logger(
            filename=log_filename, name="base_schema_manager"
        )

        # Cache thread-safe pour optimiser les accès aux métadonnées
        self._metadata_cache: nw.DataFrame[Any] | None = None
        self._cache_lock = threading.RLock()

    # Méthode de qualification d'un nom de table par le schéma (et le catalogue)
    def _qualified(self, table: str) -> str:
        """
        Return a table name qualified by this manager's schema and catalog.

        Args:
            table: Bare table name (e.g. ``'fact_table'``, ``'metadata'``).

        Returns:
            The quoted, qualified identifier targeting :attr:`schema` (and the
            catalog alias when one is actually attached).

        Example:
            >>> manager.schema = 'predictions'
            >>> manager._qualified('fact_table')
            '"predictions"."fact_table"'
        """
        # Délégation à l'utilitaire central de qualification, en propageant l'alias
        # de catalogue effectif (None pour les connexions in-memory des tests).
        return qualify_table(table, self.schema, self._catalog)

    # Méthodes de gestion du cache des métadonnées
    # Méthode de chargement des méta-données
    def _load_current_metadata(self) -> nw.DataFrame[Any]:
        """
        Load current metadata from the database with thread-safe caching.

        Returns:
            DataFrame containing current metadata (narwhals)
        """
        with self._cache_lock:
            # Chargement de la table si elle n'est pas en cache
            if self._metadata_cache is None:
                try:
                    # Chargement via polars (backend interne) puis encapsulation
                    # narwhals
                    self._metadata_cache = nw.from_native(
                        self.conn.execute(
                            f"SELECT * FROM {self._qualified('metadata')}"
                        ).pl(),
                        eager_only=True,
                    )
                except Exception:
                    # Table absente : DataFrame vide typé sur le schéma cible.
                    # Les dtypes explicites sont indispensables pour que les filtres
                    # booléens des appelants restent valides sur un frame vide.
                    self._metadata_cache = nw.from_native(
                        pl.DataFrame(
                            schema={
                                "name": pl.String,
                                "label": pl.String,
                                "sql_type": pl.String,
                                "is_categorical": pl.Boolean,
                                "is_categorical_forced": pl.Boolean,
                                "is_primary_key": pl.Boolean,
                            }
                        ),
                        eager_only=True,
                    )

            return self._metadata_cache.clone()

    # Méthode d'invalidation des méta-données mises en cache
    def _invalidate_metadata_cache(self) -> None:
        """
        Invalidate the metadata cache to force reload on next access.
        """
        with self._cache_lock:
            self._metadata_cache = None

    # Méthodes d'introspection de la base de données
    # Méthode d'extraction des colonnes de la table des faits
    # /!\ Doit être cohérent avec les colonnes dans meta-données[name]
    def _get_fact_table_columns(self) -> list[str]:
        """
        Get list of columns in fact table.

        Returns:
            List of column names
        """
        # Exécution de la requête
        result = self.conn.execute(
            f"DESCRIBE {self._qualified('fact_table')}"
        ).fetchall()
        return [row[0] for row in result]

    # Méthode d'extraction des colonnes catégorielles
    def _get_categorical_columns(self) -> list[str]:
        """
        Get list of categorical columns from metadata.

        Returns:
            List of categorical column names
        """
        # Exécution de la requête
        result = self.conn.execute(
            f"SELECT name FROM {self._qualified('metadata')} "
            "WHERE is_categorical IS TRUE"
        ).fetchall()
        return [row[0] for row in result]

    # Méthode de vérification de l'existance d'une table dans la base de données
    def _table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists in the database.

        Args:
            table_name: Name of the table to check

        Returns:
            True if table exists
        """
        # Exécution de la requête.
        # Filtrage par schéma indispensable : la même table (ex. 'fact_table') peut
        # exister dans plusieurs schémas du catalogue ; sans ce filtre, un schéma
        # voisin produirait un faux positif.
        row = self.conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_name = ? AND table_schema = ?",
            [table_name, self.schema],
        ).fetchone()
        return row[0] > 0 if row is not None else False

    # Méthode de vérification de l'existence d'une colonne dans une table
    def _column_exists(self, column: str, table: str = "fact_table") -> bool:
        """
        Check if a column exists in specified table.

        Args:
            column: Column name
            table: Table name (defaults to fact_table)

        Returns:
            True if column exists
        """
        # Extraction des colonnes de la table (qualifiée par le schéma)
        columns = [
            row[0]
            for row in self.conn.execute(
                f"DESCRIBE {self._qualified(table)}"
            ).fetchall()
        ]
        return column in columns

    # Méthode de vérification du statut catégoriel d'une colonne
    def _is_categorical_column(self, column: str) -> bool:
        """
        Check if a column is flagged as categorical in the metadata table.

        The flag is UI metadata only: it says the column is browsed through a menu,
        never that its values are stored differently.

        Args:
            column: Column name

        Returns:
            True if column is categorical

        Example:
            >>> manager._is_categorical_column('region')
            True
        """
        # Recherche du statut catégoriel
        result = self.conn.execute(
            f"SELECT is_categorical FROM {self._qualified('metadata')} WHERE name = ?",
            [column],
        ).fetchone()
        return result[0] if result else False

    # Méthode de vérification si une colonne est marquée comme clé primaire
    def _is_primary_key_column(self, column: str) -> bool:
        """
        Check if a column is marked as primary key in metadata.

        Args:
            column: Column name to check

        Returns:
            True if column is a primary key

        Example:
            >>> manager._is_primary_key_column('user_id')
            True
        """
        # Recherche du statut de clé primaire dans les méta-données
        result = self.conn.execute(
            f"SELECT is_primary_key FROM {self._qualified('metadata')} WHERE name = ?",
            [column],
        ).fetchone()
        return result[0] if result else False

    # Méthode d'extraction de toutes les colonnes marquées comme clés primaires
    def _get_primary_key_columns(self) -> list[str]:
        """
        Get all column names that are marked as primary keys in metadata.

        Returns:
            List of primary key column names. Empty list if no primary keys defined.

        Example:
            >>> manager._get_primary_key_columns()
            ['user_id', 'timestamp']
        """
        # Requête pour récupérer les noms des colonnes marquées comme clés primaires
        result = self.conn.execute(
            f"SELECT name FROM {self._qualified('metadata')} "
            "WHERE is_primary_key IS TRUE"
        ).fetchall()
        return [row[0] for row in result]

    # Méthodes de gestion des métadonnées
    # Méthode d'ajout d'une colonne aux méta-données
    def _add_column_to_metadata(
        self, column: str, df: IntoDataFrame, label: str | None = None
    ) -> None:
        """
        Add a new column to the metadata table, or refresh an existing row.

        On an existing row the producer-owned fields are preserved: a ``label``
        already recorded is never overwritten by a data update, and a column whose
        categorical status was forced keeps it.

        Args:
            column: Column name
            df: DataFrame containing the column (any narwhals-compatible backend)
            label: Custom label for the column. If None, a label is derived from the
                column name on insertion, and the recorded label is kept on update.

        Example:
            >>> manager._add_column_to_metadata('score', df)
        """
        # Conversion vers narwhals pour un accès uniforme au schéma
        df_nw = nw.from_native(df, eager_only=True)
        # Extraction du type narwhals de la colonne
        dtype_obj = df_nw.schema[column]
        # Conversion du type narwhals en SQL
        sql_type = map_python_to_sql_type(dtype_obj)
        # Statut catégoriel : colonne textuelle dont la cardinalité respecte le seuil.
        # Les valeurs manquantes sont exclues du comptage des modalités.
        is_categorical = (
            isinstance(dtype_obj, (nw.String, nw.Categorical, nw.Enum))
            and self.categorical_threshold is not None
            and df_nw[column].drop_nulls().n_unique() <= self.categorical_threshold
        )

        # Nom qualifié de la table de métadonnées
        metadata_table = self._qualified("metadata")

        # Création de la table metadata si elle n'existe pas.
        self.conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {metadata_table} (
                name VARCHAR,
                label VARCHAR,
                sql_type VARCHAR,
                is_categorical BOOLEAN,
                is_categorical_forced BOOLEAN DEFAULT FALSE,
                is_primary_key BOOLEAN DEFAULT FALSE
            )
        """)

        # Upsert manuel
        # Vérification de l'existence de la colonne avant d'insérer ou de mettre à jour.
        _row = self.conn.execute(
            f"SELECT COUNT(*) FROM {metadata_table} WHERE name = ?", [column]
        ).fetchone()
        existing_count = _row[0] if _row is not None else 0

        if existing_count == 0:
            # Colonne absente : insertion.
            # Libellé par défaut dérivé du nom technique, statut jamais forcé (la
            # colonne est découverte, pas déclarée par le producteur).
            insert_label = (
                label if label is not None else column.replace("_", " ").title()
            )
            self.conn.execute(
                f"""
                INSERT INTO {metadata_table} (name, label, sql_type,
                is_categorical, is_categorical_forced, is_primary_key)
                VALUES (?, ?, ?, ?, FALSE, FALSE)
                """,
                [column, insert_label, sql_type, is_categorical],
            )
        else:
            # Colonne déjà présente : mise à jour des seuls champs dérivés des données.
            # COALESCE préserve un libellé déjà renseigné par le producteur ; le CASE
            # protège un statut catégoriel explicitement forcé.
            self.conn.execute(
                f"""
                UPDATE {metadata_table}
                SET label = COALESCE(?, label),
                    sql_type = ?,
                    is_categorical = CASE WHEN is_categorical_forced
                                          THEN is_categorical ELSE ? END
                WHERE name = ?
                """,
                [label, sql_type, is_categorical, column],
            )

        # Invalidation du cache
        self._invalidate_metadata_cache()

        # Logging
        self.logger.info(f"Added/updated column {column} in metadata")

    # Méthode de mise à jour du statut catégoriel d'une donnée
    def _update_categorical_status(self, col_name: str, is_categorical: bool) -> None:
        """
        Update categorical status in metadata.

        Args:
            col_name: Column name
            is_categorical: New categorical status
        """
        # Exécution de la requête de mise à jour
        self.conn.execute(
            f"UPDATE {self._qualified('metadata')} "
            "SET is_categorical = ? WHERE name = ?",
            [is_categorical, col_name],
        )

        # Invalidation du cache
        self._invalidate_metadata_cache()

        # Logging
        self.logger.info(f"Updated categorical status for {col_name}: {is_categorical}")

    # Méthode de suppression des méta-données pour une colonne
    def delete_column_metadata(self, column_name: str) -> None:
        """
        Delete metadata for a specific column.

        Args:
            column_name: Name of the column
        """
        try:
            # Requête de suppression des méta-données
            delete_query = f"DELETE FROM {self._qualified('metadata')} WHERE name = ?"
            # Exécution de la requête
            self.conn.execute(delete_query, [column_name])

            # Invalidation du cache
            self._invalidate_metadata_cache()

            # Logging
            self.logger.info(f"Deleted metadata for column {column_name}")

        except Exception as e:
            # Logging
            self.logger.error(
                f"Failed to delete metadata for column {column_name}: {e}"
            )
            raise

    # Méthodes de résolution des conflits de types
    def _resolve_type_conflicts(
        self, column: str, df: nw.DataFrame[Any], current_metadata: nw.DataFrame[Any]
    ) -> None:
        """
        Resolve a type conflict on SQL types, widening only.

        The recorded ``metadata.sql_type`` is never narrowed: a stored ``BIGINT``
        survives a batch of ``Int32``. Non-ordered types (temporal, decimal, binary)
        are left untouched and reported. The fact table column is widened alongside
        the metadata so both stay consistent.

        Args:
            column: Column name
            df: DataFrame with new data (narwhals)
            current_metadata: Current metadata (narwhals)

        Example:
            >>> manager._resolve_type_conflicts('amount', df, metadata)
        """
        # Identification du type SQL actuellement enregistré
        matching = current_metadata.filter(nw.col("name") == column)["sql_type"]
        if len(matching) == 0:
            return None
        current_type = str(matching[0])

        # Conservation du type existant si toutes les nouvelles valeurs sont nulles :
        # narwhals infère alors 'Null', que map_python_to_sql_type replie sur VARCHAR,
        # ce qui promouvrait à tort une colonne numérique connue en texte.
        if df[column].is_null().all():
            return None

        # Type SQL du lot entrant
        new_type = map_python_to_sql_type(df.schema[column])
        # Ne fait rien si inchangé
        if current_type == new_type:
            return None

        # Résolution par élargissement uniquement
        resolved_type = resolve_sql_type_conflict(current_type, new_type)
        if resolved_type is None:
            # Type non ordonné ou lot plus étroit : le type enregistré est conservé
            self.logger.info(
                f"Type conflict for {column}: incoming {new_type} does not widen"
                f" stored {current_type}; metadata left unchanged"
            )
            return None

        # Élargissement de la colonne de la table des faits, pour que le type
        # enregistré et le type physique restent cohérents (contrôlé par l'auditeur).
        # Échec non bloquant : la métadonnée reste la référence déclarative.
        try:
            self.conn.execute(
                f"ALTER TABLE {self._qualified('fact_table')}"
                f" ALTER {quote_ident(column)} SET DATA TYPE {resolved_type}"
            )
        except Exception as e:
            self.logger.warning(
                f"Could not widen fact_table.{column} to {resolved_type}: {e}"
            )

        # Mise à jour du type SQL enregistré
        self.conn.execute(
            f"""
            UPDATE {self._qualified("metadata")}
            SET sql_type = ?
            WHERE name = ?
        """,
            [resolved_type, column],
        )
        # Invalidation du cache
        self._invalidate_metadata_cache()
        # Logging
        self.logger.info(
            f"Type conflict resolution for {column}: {current_type} ->"
            f" {resolved_type}"
        )

    # Méthode utilitaire pour les colonnes contenant uniquement des valeurs nulles
    def _get_null_only_columns(self) -> list[str]:
        """
        Get list of columns that contain only null values in the fact table.

        Returns:
            List of column names that contain only null values
        """
        # Initialisation de la liste des colonnes vides
        null_only_columns = []

        try:
            # Récupération des colonnes de la fact table
            columns = self._get_fact_table_columns()
            # Parcours des données
            for column in columns:
                # Vérification si la colonne ne contient que des valeurs nulles
                query = (
                    f"SELECT COUNT(*) FROM {self._qualified('fact_table')} "
                    f"WHERE {quote_ident(column)} IS NOT NULL"
                )
                _row = self.conn.execute(query).fetchone()
                non_null_count = _row[0] if _row is not None else 0
                # Ajout à la liste si ne contient que des colonnes nulles
                if non_null_count == 0:
                    null_only_columns.append(column)
            # Logging
            if null_only_columns:
                self.logger.info(
                    f"Columns containing only null values detected: {null_only_columns}"
                )
            return null_only_columns

        except Exception as e:
            self.logger.error(f"An error occurred while detecting null values: {e}")
            raise

    # Méthode d'actualisation du statut catégoriel des colonnes textuelles
    def _refresh_categorical_flags(self) -> list[str]:
        """
        Recompute the ``is_categorical`` flag of every eligible VARCHAR column.

        The status is pure UI metadata: it is derived from the current distinct
        count of the fact table compared against ``categorical_threshold``, and only
        a plain ``UPDATE metadata`` is issued — the fact table is never rewritten.
        Columns whose status was forced by the producer
        (``is_categorical_forced``) are skipped, and an ``UPDATE`` is emitted only
        when the boolean actually changes.

        Returns:
            List of column names whose flag was flipped. Empty when nothing changed
            or when no threshold is configured.

        Example:
            >>> manager._refresh_categorical_flags()
            ['high_cardinality']
        """
        # Liste des colonnes dont le statut a effectivement basculé
        changed: list[str] = []

        # Absence de seuil : le statut catégoriel n'est pas ré-évaluable
        if self.categorical_threshold is None:
            return changed

        try:
            # Chargement des méta-données courantes
            current_metadata = self._load_current_metadata()
            if len(current_metadata) == 0:
                return changed

            # Sélection des colonnes textuelles dont le statut n'a pas été forcé
            candidates = current_metadata.filter(
                (nw.col("sql_type") == "VARCHAR")
                & (~nw.col("is_categorical_forced"))
            )

            # Colonnes réellement présentes dans la table des faits
            fact_columns = set(self._get_fact_table_columns())
            # Nom qualifié de la table des faits
            fact_table = self._qualified("fact_table")

            for col_name, was_categorical in zip(
                candidates["name"].to_list(),
                candidates["is_categorical"].to_list(),
            ):
                # Colonne absente de la table des faits : rien à recalculer
                if col_name not in fact_columns:
                    continue

                # Comptage des modalités sur l'état courant de la table des faits
                quoted_col = quote_ident(col_name)
                row = self.conn.execute(
                    f"SELECT COUNT(DISTINCT {quoted_col}) FROM {fact_table}"
                    f" WHERE {quoted_col} IS NOT NULL"
                ).fetchone()
                n_distinct = int(row[0]) if row is not None else 0

                # Statut attendu : au moins une modalité observée et seuil respecté.
                # Une colonne entièrement nulle n'est pas catégorielle, l'absence de
                # modalités ne constituant pas une information de cardinalité.
                is_categorical = 0 < n_distinct <= self.categorical_threshold

                # Écriture uniquement en cas de bascule effective
                if bool(was_categorical) is is_categorical:
                    continue

                # Mise à jour du booléen et invalidation du cache
                self._update_categorical_status(col_name, is_categorical)
                changed.append(col_name)
                # Logging
                self.logger.info(
                    f"Categorical status of '{col_name}' set to {is_categorical}"
                    f" ({n_distinct} distinct values, threshold"
                    f" {self.categorical_threshold})"
                )

            return changed

        except Exception as e:
            # Logging
            self.logger.error(f"Error refreshing categorical flags: {e}")
            return changed

    # Méthode d'horodatage de la dernière écriture réussie
    def _touch_dataset_metadata(self) -> None:
        """
        Stamp ``dataset_metadata.updated_at`` with the current timestamp.

        The table holds exactly one row per schema, so no ``WHERE`` clause is
        needed. Failure is non-blocking: the timestamp is descriptive metadata and
        must never invalidate an otherwise successful write.

        Example:
            >>> manager._touch_dataset_metadata()
        """
        try:
            # Horodatage de la dernière écriture réussie.
            # Valeur liée en Python plutôt que via now() : la colonne est un
            # TIMESTAMP sans fuseau, là où now() renvoie un TIMESTAMP WITH TIME ZONE.
            self.conn.execute(
                f"UPDATE {self._qualified('dataset_metadata')} SET updated_at = ?",
                [datetime.now()],
            )
        except Exception as e:
            # Erreur non bloquante : l'horodatage ne conditionne pas l'écriture
            self.logger.warning(f"Could not stamp dataset_metadata.updated_at: {e}")

    @abstractmethod
    def validate_operation(self, operation_type: str, **kwargs: Any) -> bool:
        """
        Abstract method to validate operations before execution.

        Args:
            operation_type: Type of operation to validate
            **kwargs: Operation-specific parameters

        Returns:
            True if operation is valid
        """
        pass

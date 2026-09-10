# Importation des modules
# Modules de base
import duckdb
import narwhals as nw
import polars as pl

# Module de tests
import pytest

# Utilisation de DataManager (sous-classe concrète) pour instancier
# BaseSchemaManager
from dt_ducklake_manager._internal.managers.data import DataManager

# ---------------------------------------------------------------------------
# Fixture locale
# ---------------------------------------------------------------------------


# Initialisation d'une instance concrète de BaseSchemaManager pour les tests
@pytest.fixture
def manager(built_ducklake_schema: duckdb.DuckDBPyConnection) -> DataManager:
    """Create a DataManager (concrete subclass) to test BaseSchemaManager methods.

    Args:
        built_ducklake_schema: Fixture providing a DuckDB connection with a built
        schema.

    Returns:
        DataManager: initialized with the test connection.
    """
    return DataManager(connection=built_ducklake_schema, categorical_threshold=4)


# ===========================================================================
# Tests de _load_current_metadata()
# ===========================================================================


# Test que _load_current_metadata retourne un DataFrame narwhals avec les colonnes
# attendues
def test_load_current_metadata_returns_dataframe(manager: DataManager) -> None:
    """Test that _load_current_metadata returns a narwhals DataFrame
    with expected columns.

    Args:
        manager: DataManager fixture with a built schema.
    """
    metadata = manager._load_current_metadata()
    # Vérification du type de retour
    assert isinstance(metadata, nw.DataFrame)
    # Vérification de la présence des colonnes de métadonnées
    assert "name" in metadata.columns
    assert "is_categorical" in metadata.columns
    assert "is_primary_key" in metadata.columns


# Test que _load_current_metadata retourne un DataFrame non vide pour un schéma
# construit
def test_load_current_metadata_non_empty(manager: DataManager) -> None:
    """Test that _load_current_metadata returns a non-empty DataFrame
    for a built schema.

    Args:
        manager: DataManager fixture with a built schema.
    """
    metadata = manager._load_current_metadata()
    # Le schéma built_ducklake_schema a été construit avec sample_df qui a 6 colonnes
    assert len(metadata) > 0


# ===========================================================================
# Tests de _table_exists()
# ===========================================================================


# Test que _table_exists retourne True pour une table existante
def test_table_exists_fact_table(manager: DataManager) -> None:
    """Test that _table_exists returns True for an existing table.

    Args:
        manager: DataManager fixture with a built schema.
    """
    assert manager._table_exists("fact_table") is True


# Test que _table_exists retourne True pour la table metadata
def test_table_exists_metadata(manager: DataManager) -> None:
    """Test that _table_exists returns True for the metadata table.

    Args:
        manager: DataManager fixture with a built schema.
    """
    assert manager._table_exists("metadata") is True


# Test que _table_exists retourne False pour une table inexistante
def test_table_exists_nonexistent(manager: DataManager) -> None:
    """Test that _table_exists returns False for a non-existent table.

    Args:
        manager: DataManager fixture with a built schema.
    """
    assert manager._table_exists("nonexistent_table_xyz") is False


# ===========================================================================
# Tests de _get_primary_key_columns()
# ===========================================================================


# Test que _get_primary_key_columns retourne la liste des clés primaires
def test_get_primary_key_columns(manager: DataManager) -> None:
    """Test that _get_primary_key_columns returns the list of primary key columns.

    Args:
        manager: DataManager fixture with a built schema.
    """
    pks = manager._get_primary_key_columns()
    # Vérification du type de retour
    assert isinstance(pks, list)
    # Le schéma est construit avec primary_keys=['id'] dans built_ducklake_schema
    assert "id" in pks


# ===========================================================================
# Tests de _column_exists()
# ===========================================================================


# Test que _column_exists retourne True pour une colonne existante dans fact_table
def test_column_exists_true(manager: DataManager) -> None:
    """Test that _column_exists returns True for an existing column.

    Args:
        manager: DataManager fixture with a built schema.
    """
    assert manager._column_exists("id", "fact_table") is True


# Test que _column_exists retourne False pour une colonne inexistante
def test_column_exists_false(manager: DataManager) -> None:
    """Test that _column_exists returns False for a non-existent column.

    Args:
        manager: DataManager fixture with a built schema.
    """
    assert manager._column_exists("nonexistent_col", "fact_table") is False


# ===========================================================================
# Tests de _is_primary_key_column()
# ===========================================================================


# Test que _is_primary_key_column retourne True pour une colonne clé primaire
def test_is_primary_key_column_true(manager: DataManager) -> None:
    """Test that _is_primary_key_column returns True for a primary key column.

    Args:
        manager: DataManager fixture with a built schema.
    """
    assert manager._is_primary_key_column("id") is True


# Test que _is_primary_key_column retourne False pour une colonne non-clé primaire
def test_is_primary_key_column_false(manager: DataManager) -> None:
    """Test that _is_primary_key_column returns False for a non-primary-key column.

    Args:
        manager: DataManager fixture with a built schema.
    """
    assert manager._is_primary_key_column("value") is False


# ===========================================================================
# Tests de _invalidate_metadata_cache()
# ===========================================================================


# Test que _invalidate_metadata_cache vide le cache
def test_invalidate_metadata_cache(manager: DataManager) -> None:
    """Test that _invalidate_metadata_cache sets the cache to None.

    Args:
        manager: DataManager fixture with a built schema.
    """
    # Chargement du cache
    _ = manager._load_current_metadata()
    assert manager._metadata_cache is not None

    # Invalidation du cache
    manager._invalidate_metadata_cache()
    assert manager._metadata_cache is None


# ===========================================================================
# Tests de _refresh_categorical_flags()
# ===========================================================================


# Test qu'une colonne passant sous le seuil devient catégorielle
def test_refresh_categorical_flags_becomes_categorical(
    manager: DataManager, built_ducklake_schema: duckdb.DuckDBPyConnection
) -> None:
    """Test that a column falling to or below the threshold becomes categorical.

    'high_cardinality' starts with 5 distinct values for a threshold of 4. Deleting
    the row carrying the fifth modality brings it to 4, i.e. exactly the threshold.

    Args:
        manager: DataManager fixture with a built schema.
        built_ducklake_schema: DuckDB connection with the built schema.
    """
    # Vérification de l'état initial : non catégorielle
    assert manager._is_categorical_column("high_cardinality") is False

    # Suppression de l'unique porteur de la 5e modalité
    built_ducklake_schema.execute("DELETE FROM fact_table WHERE id = 5")

    # Actualisation du statut catégoriel
    changed = manager._refresh_categorical_flags()

    assert "high_cardinality" in changed
    assert manager._is_categorical_column("high_cardinality") is True


# Test qu'une colonne dépassant le seuil cesse d'être catégorielle
def test_refresh_categorical_flags_becomes_non_categorical(
    manager: DataManager, built_ducklake_schema: duckdb.DuckDBPyConnection
) -> None:
    """Test that a column exceeding the threshold stops being categorical.

    'category' starts with 3 distinct values for a threshold of 4; inserting two
    unseen modalities brings it to 5.

    Args:
        manager: DataManager fixture with a built schema.
        built_ducklake_schema: DuckDB connection with the built schema.
    """
    # Vérification de l'état initial : catégorielle
    assert manager._is_categorical_column("category") is True

    # Ajout de deux modalités inédites, portant le total à 5 > seuil = 4
    built_ducklake_schema.execute(
        "INSERT INTO fact_table (id, category) VALUES (10, 'D'), (11, 'E')"
    )

    # Actualisation du statut catégoriel
    changed = manager._refresh_categorical_flags()

    assert "category" in changed
    assert manager._is_categorical_column("category") is False


# Test qu'une colonne au statut forcé n'est jamais rebasculée
def test_refresh_categorical_flags_skips_forced_column(
    manager: DataManager, built_ducklake_schema: duckdb.DuckDBPyConnection
) -> None:
    """Test that a column whose categorical status was forced is never re-evaluated.

    Args:
        manager: DataManager fixture with a built schema.
        built_ducklake_schema: DuckDB connection with the built schema.
    """
    # Forçage du statut catégoriel de 'high_cardinality', au-delà du seuil
    built_ducklake_schema.execute(
        "UPDATE metadata SET is_categorical = TRUE, is_categorical_forced = TRUE"
        " WHERE name = 'high_cardinality'"
    )
    manager._invalidate_metadata_cache()

    # Actualisation du statut catégoriel
    changed = manager._refresh_categorical_flags()

    # La colonne forcée est ignorée, son statut est conservé
    assert "high_cardinality" not in changed
    assert manager._is_categorical_column("high_cardinality") is True


# Test qu'aucun statut n'est recalculé en l'absence de seuil
def test_refresh_categorical_flags_without_threshold(
    built_ducklake_schema: duckdb.DuckDBPyConnection,
) -> None:
    """Test that no flag is recomputed when no threshold is configured.

    Args:
        built_ducklake_schema: DuckDB connection with the built schema.
    """
    mgr = DataManager(connection=built_ducklake_schema, categorical_threshold=None)
    assert mgr._refresh_categorical_flags() == []
    # Le statut initial reste inchangé
    assert mgr._is_categorical_column("category") is True


# ===========================================================================
# Tests de _resolve_type_conflicts()
# ===========================================================================


# Test qu'un BIGINT enregistré n'est pas rétrogradé par un lot d'Int32
def test_resolve_type_conflicts_keeps_widest_integer(
    manager: DataManager, built_ducklake_schema: duckdb.DuckDBPyConnection
) -> None:
    """Test that a stored BIGINT is not downgraded by a batch of Int32.

    Args:
        manager: DataManager fixture with a built schema.
        built_ducklake_schema: DuckDB connection with the built schema.
    """
    # 'id' est enregistrée en BIGINT (polars infère Int64)
    metadata = manager._load_current_metadata()
    df = nw.from_native(
        pl.DataFrame({"id": pl.Series("id", [1, 2, 3], dtype=pl.Int32)}),
        eager_only=True,
    )

    manager._resolve_type_conflicts("id", df, metadata)

    # Le type le plus large est conservé
    stored = built_ducklake_schema.execute(
        "SELECT sql_type FROM metadata WHERE name = 'id'"
    ).fetchone()
    assert stored is not None
    assert stored[0] == "BIGINT"


# Test qu'un lot plus large élargit le type enregistré
def test_resolve_type_conflicts_widens_to_varchar(
    manager: DataManager, built_ducklake_schema: duckdb.DuckDBPyConnection
) -> None:
    """Test that a textual batch widens a numeric column to VARCHAR.

    Args:
        manager: DataManager fixture with a built schema.
        built_ducklake_schema: DuckDB connection with the built schema.
    """
    metadata = manager._load_current_metadata()
    df = nw.from_native(
        pl.DataFrame({"value": ["a", "b", "c"]}),
        eager_only=True,
    )

    manager._resolve_type_conflicts("value", df, metadata)

    stored = built_ducklake_schema.execute(
        "SELECT sql_type FROM metadata WHERE name = 'value'"
    ).fetchone()
    assert stored is not None
    assert stored[0] == "VARCHAR"


# Test qu'un lot entièrement nul ne modifie jamais le type enregistré
def test_resolve_type_conflicts_ignores_all_null_batch(
    manager: DataManager, built_ducklake_schema: duckdb.DuckDBPyConnection
) -> None:
    """Test that an all-null batch never overwrites a known numeric type.

    Without the guard, narwhals would infer 'Null', which maps to VARCHAR and would
    wrongly promote the column.

    Args:
        manager: DataManager fixture with a built schema.
        built_ducklake_schema: DuckDB connection with the built schema.
    """
    metadata = manager._load_current_metadata()
    df = nw.from_native(
        pl.DataFrame({"value": pl.Series("value", [None, None], dtype=pl.Null)}),
        eager_only=True,
    )

    manager._resolve_type_conflicts("value", df, metadata)

    stored = built_ducklake_schema.execute(
        "SELECT sql_type FROM metadata WHERE name = 'value'"
    ).fetchone()
    assert stored is not None
    assert stored[0] == "DOUBLE"


# ===========================================================================
# Tests du support multi-schémas (qualification et isolation)
# ===========================================================================


# Test que _qualified préfixe le nom de table par le schéma du gestionnaire
def test_qualified_prefixes_schema(
    built_ducklake_schema: duckdb.DuckDBPyConnection,
) -> None:
    """Test that _qualified prefixes the table name with the manager's schema.

    The in-memory test connection has no attached catalog, so ``_qualified``
    quotes and schema-qualifies the name without a catalog prefix.

    Args:
        built_ducklake_schema: Fixture providing a DuckDB connection with a built
        schema.
    """
    mgr = DataManager(connection=built_ducklake_schema, schema="predictions")
    assert mgr._qualified("fact_table") == '"predictions"."fact_table"'
    assert mgr._qualified("dataset_metadata") == '"predictions"."dataset_metadata"'


# Test que le schéma par défaut est 'main'
def test_default_schema_is_main(
    built_ducklake_schema: duckdb.DuckDBPyConnection,
) -> None:
    """Test that the default schema is 'main'.

    Args:
        built_ducklake_schema: Fixture providing a DuckDB connection with a built
        schema.
    """
    mgr = DataManager(connection=built_ducklake_schema)
    assert mgr.schema == "main"
    assert mgr._qualified("metadata") == '"main"."metadata"'


# Test que l'alias du catalogue est conservé au même titre que le schéma
def test_catalog_alias_default_and_custom(
    built_ducklake_schema: duckdb.DuckDBPyConnection,
) -> None:
    """Test that ``catalog_alias`` defaults to 'db' and is stored when provided.

    The base manager carries the catalog alias alongside the schema so that later
    passes can qualify table references by the catalog.

    Args:
        built_ducklake_schema: Fixture providing a DuckDB connection with a built
        schema.
    """
    # Valeur par défaut
    default_mgr = DataManager(connection=built_ducklake_schema)
    assert default_mgr.catalog_alias == "db"

    # Valeur explicite propagée jusqu'à la classe de base
    custom_mgr = DataManager(
        connection=built_ducklake_schema, catalog_alias="my_lake"
    )
    assert custom_mgr.catalog_alias == "my_lake"


# Test que _table_exists est isolé par schéma : une table d'un schéma n'est pas vue
# depuis un autre schéma du même catalogue
def test_table_exists_isolated_by_schema(
    multi_schema_connection: duckdb.DuckDBPyConnection,
) -> None:
    """Test that _table_exists distinguishes tables across schemas.

    A ``fact_table`` exists in both schemas; a manager bound to one schema must not
    see tables of a schema where they were not built (here a third, empty schema).

    Args:
        multi_schema_connection: Connection with 'predictions' and 'shapley' schemas.
    """
    # Gestionnaires liés à chacun des deux schémas construits
    pred_mgr = DataManager(
        connection=multi_schema_connection, schema="predictions"
    )
    shap_mgr = DataManager(connection=multi_schema_connection, schema="shapley")

    # Chaque schéma voit bien ses propres tables
    assert pred_mgr._table_exists("fact_table") is True
    assert shap_mgr._table_exists("fact_table") is True

    # Un schéma vide ne voit aucune table fact_table
    empty_mgr = DataManager(connection=multi_schema_connection, schema="main")
    assert empty_mgr._table_exists("fact_table") is False


# Test que les métadonnées chargées sont propres au schéma ciblé
def test_metadata_isolated_by_schema(
    multi_schema_connection: duckdb.DuckDBPyConnection,
) -> None:
    """Test that _load_current_metadata reads the targeted schema's metadata only.

    The 'shapley' schema carries a 'shap_value' column absent from 'predictions'.

    Args:
        multi_schema_connection: Connection with 'predictions' and 'shapley' schemas.
    """
    pred_mgr = DataManager(
        connection=multi_schema_connection, schema="predictions"
    )
    shap_mgr = DataManager(connection=multi_schema_connection, schema="shapley")

    pred_columns = set(pred_mgr._load_current_metadata()["name"].to_list())
    shap_columns = set(shap_mgr._load_current_metadata()["name"].to_list())

    # La colonne 'value' n'existe que dans predictions, 'shap_value' que dans shapley
    assert "value" in pred_columns
    assert "value" not in shap_columns
    assert "shap_value" in shap_columns
    assert "shap_value" not in pred_columns

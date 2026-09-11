# Importation des modules
# Modules de base
import os
import warnings

# Module de tests
from typing import Any

import duckdb
import polars as pl

# Module de tests
import pytest

# Modules du package à tester
from dt_ducklake_manager.connection import DuckLakeConnector
from dt_ducklake_manager.operations import DatabaseDeleter
from dt_ducklake_manager.schema import DuckLakeTablesBuilder


def _ducklake_available() -> bool:
    """Vérifie si l'extension DuckLake est disponible dans l'environnement de test."""
    try:
        conn = duckdb.connect(":memory:")
        conn.execute("INSTALL ducklake; LOAD ducklake;")
        conn.close()
        return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Tests de l'initialisation
# ---------------------------------------------------------------------------


# Test de l'initialisation correcte de DatabaseDeleter
def test_deleter_initialization(built_ducklake_schema: Any) -> None:
    """Test that DatabaseDeleter initializes without errors.

    Args:
        built_ducklake_schema: Fixture providing a DuckDB connection with a built
        schema.
    """
    deleter = DatabaseDeleter(connection=built_ducklake_schema, categorical_threshold=4)
    assert deleter is not None
    assert deleter.categorical_threshold == 4


# Test de l'initialisation avec enable_validation=False
def test_deleter_initialization_without_validation(built_ducklake_schema: Any) -> None:
    """Test that DatabaseDeleter can be initialized with validation disabled.

    Args:
        built_ducklake_schema: Fixture providing a DuckDB connection with a built
        schema.
    """
    deleter = DatabaseDeleter(connection=built_ducklake_schema, enable_validation=False)
    # Vérification que l'auditeur n'est pas initialisé
    assert deleter.enable_validation is False


# Test que catalog_alias est propagé aux sous-gestionnaires
def test_deleter_propagates_catalog_alias(built_ducklake_schema: Any) -> None:
    """Test that ``catalog_alias`` reaches every specialized sub-manager.

    Args:
        built_ducklake_schema: Fixture providing a DuckDB connection with a built
        schema.
    """
    deleter = DatabaseDeleter(connection=built_ducklake_schema, catalog_alias="my_lake")
    assert deleter.catalog_alias == "my_lake"
    assert deleter.data_mgr.catalog_alias == "my_lake"
    assert deleter.transaction_mgr.catalog_alias == "my_lake"
    assert deleter.auditor is not None
    assert deleter.auditor.catalog_alias == "my_lake"


# Test que catalog_alias vaut 'db' par défaut
def test_deleter_default_catalog_alias(built_ducklake_schema: Any) -> None:
    """Test that ``catalog_alias`` defaults to 'db'.

    Args:
        built_ducklake_schema: Fixture providing a DuckDB connection with a built
        schema.
    """
    deleter = DatabaseDeleter(connection=built_ducklake_schema)
    assert deleter.catalog_alias == "db"


# ---------------------------------------------------------------------------
# Tests de validate_operation()
# ---------------------------------------------------------------------------


# Test que validate_operation retourne un booléen pour une suppression valide
def test_validate_operation_delete_returns_bool(deleter: DatabaseDeleter) -> None:
    """Test that validate_operation returns a boolean for a delete operation.

    Args:
        deleter: DatabaseDeleter fixture.
    """
    result = deleter.validate_operation("delete", filters=[("id", "=", 1)])
    assert isinstance(result, bool)


# ---------------------------------------------------------------------------
# Tests de delete_rows()
# ---------------------------------------------------------------------------


# Test de la suppression de lignes avec un filtre simple
def test_delete_rows_with_filter(
    deleter: DatabaseDeleter, built_ducklake_schema: Any
) -> None:
    """Test that delete_rows removes rows matching the given filter.

    Args:
        deleter: DatabaseDeleter fixture.
        built_ducklake_schema: DuckDB connection.
    """
    # Vérification que la ligne id=1 existe avant suppression
    before = built_ducklake_schema.execute(
        "SELECT COUNT(*) FROM fact_table WHERE id = 1"
    ).fetchone()[0]
    assert before >= 1

    # Suppression de la ligne avec id=1
    deleted_count = deleter.delete_rows(
        filters=[("id", "=", 1)],
        use_transaction=False,
    )

    # Vérification que le nombre de lignes supprimées est cohérent
    assert isinstance(deleted_count, int)
    assert deleted_count >= 1

    # Vérification que la ligne est bien absente
    after = built_ducklake_schema.execute(
        "SELECT COUNT(*) FROM fact_table WHERE id = 1"
    ).fetchone()[0]
    assert after == 0


# Test de la suppression de lignes avec un filtre OR (liste de listes de tuples)
def test_delete_rows_with_or_filter(
    deleter: DatabaseDeleter, built_ducklake_schema: Any
) -> None:
    """Test that delete_rows handles OR filters (list of lists of tuples).

    Args:
        deleter: DatabaseDeleter fixture.
        built_ducklake_schema: DuckDB connection.
    """
    # Suppression des lignes avec id=2 OU id=3
    before = built_ducklake_schema.execute(
        "SELECT COUNT(*) FROM fact_table WHERE id IN (2, 3)"
    ).fetchone()[0]
    assert before >= 1

    deleted_count = deleter.delete_rows(
        filters=[[("id", "=", 2)], [("id", "=", 3)]],
        use_transaction=False,
    )
    assert deleted_count >= 1

    # Vérification que les lignes sont bien absentes
    after = built_ducklake_schema.execute(
        "SELECT COUNT(*) FROM fact_table WHERE id IN (2, 3)"
    ).fetchone()[0]
    assert after == 0


# Test de la suppression de toutes les lignes avec filters=None
def test_delete_rows_all(deleter: DatabaseDeleter, built_ducklake_schema: Any) -> None:
    """Test that delete_rows with filters=None removes all rows.

    Args:
        deleter: DatabaseDeleter fixture.
        built_ducklake_schema: DuckDB connection.
    """
    before = built_ducklake_schema.execute(
        "SELECT COUNT(*) FROM fact_table"
    ).fetchone()[0]
    assert before > 0

    # Remarque : DatabaseDeleter n'accepte pas filters=None (validation obligatoire des
    # filtres).
    # Suppression de toutes les lignes via un filtre SQL universel.
    deleted_count = deleter.delete_rows(filters="1=1", use_transaction=False)
    assert deleted_count == before

    after = built_ducklake_schema.execute("SELECT COUNT(*) FROM fact_table").fetchone()[
        0
    ]
    assert after == 0


# ---------------------------------------------------------------------------
# Tests de delete_columns()
# ---------------------------------------------------------------------------


# Test de la suppression d'une colonne non-clé primaire
def test_delete_columns_single_column(
    deleter: DatabaseDeleter, built_ducklake_schema: Any
) -> None:
    """Test that delete_columns removes a non-primary-key column from the fact table.

    Args:
        deleter: DatabaseDeleter fixture.
        built_ducklake_schema: DuckDB connection.
    """
    # Vérification que la colonne 'value' existe avant suppression
    columns_before = [
        row[0]
        for row in built_ducklake_schema.execute("DESCRIBE fact_table").fetchall()
    ]
    assert "value" in columns_before

    result = deleter.delete_columns(["value"], use_transaction=False)

    # Vérification que le résultat est un dictionnaire de statuts
    assert isinstance(result, dict)
    assert "value" in result
    # Vérification que la colonne est bien supprimée
    columns_after = [
        row[0]
        for row in built_ducklake_schema.execute("DESCRIBE fact_table").fetchall()
    ]
    assert "value" not in columns_after


# ---------------------------------------------------------------------------
# Tests de changement de statut catégoriel lors d'une suppression
# ---------------------------------------------------------------------------


# Test de conversion non-catégorielle → catégorielle après suppression de lignes
def test_delete_rows_non_categorical_becomes_categorical(
    deleter: DatabaseDeleter, built_ducklake_schema: Any
) -> None:
    """Test that a non-categorical column becomes categorical when its
    unique value count drops to or below the threshold after rows are deleted.

    The sample schema is built with categorical_threshold=4. The column
    'high_cardinality'
    initially holds 5 unique values (val_100..val_104) and is NOT categorical. The row
    with id=5 carries the only occurrence of 'val_104'. Deleting that row leaves exactly
    4 distinct values (val_100..val_103) which equals the threshold, flipping the
    metadata flag. The fact table itself is never rewritten.

    Args:
        deleter: DatabaseDeleter fixture with auto_cleanup=True.
        built_ducklake_schema: DuckDB connection with the built schema.
    """
    # Vérification initiale : high_cardinality n'est pas catégorielle (5 valeurs >
    # seuil=4)
    is_cat_before = built_ducklake_schema.execute(
        "SELECT is_categorical FROM metadata WHERE name = 'high_cardinality'"
    ).fetchone()[0]
    assert is_cat_before is False

    # Suppression de la ligne id=5 (seul porteur de 'val_104') :
    # après suppression, high_cardinality n'aura plus que 4 valeurs uniques
    # (val_100..val_103)
    # ce qui est ≤ seuil=4 → bascule du booléen is_categorical déclenchée par le
    # nettoyage automatique (_refresh_categorical_flags via
    # _cleanup_orphaned_data_comprehensive).
    deleted_count = deleter.delete_rows(
        filters=[("id", "=", 5)],
        use_transaction=False,
    )
    assert deleted_count == 1

    # Vérification : high_cardinality est désormais catégorielle dans les métadonnées
    is_cat_after = built_ducklake_schema.execute(
        "SELECT is_categorical FROM metadata WHERE name = 'high_cardinality'"
    ).fetchone()[0]
    assert is_cat_after is True

    # Vérification : les libellés d'origine sont toujours stockés tels quels
    stored_labels = {
        row[0]
        for row in built_ducklake_schema.execute(
            "SELECT DISTINCT high_cardinality FROM fact_table"
        ).fetchall()
    }
    assert stored_labels == {"val_100", "val_101", "val_102", "val_103"}


# ---------------------------------------------------------------------------
# Tests de delete_columns() sur une colonne parente d'une hiérarchie (§2.5)
# ---------------------------------------------------------------------------


# Test que la suppression d'une colonne parente est refusée sans cascade
def test_delete_columns_parent_refused_without_cascade(
    deleter: DatabaseDeleter, built_ducklake_schema: Any
) -> None:
    """Test that deleting a hierarchy parent column is refused by default.

    'status' is declared as the parent of 'category'; deleting 'status' without
    cascade=True must be refused for the whole batch and leave both columns intact.

    Args:
        deleter: DatabaseDeleter fixture.
        built_ducklake_schema: DuckDB connection.
    """
    # 'category' et 'status' sont déjà catégorielles (seuil=4) : aucun forçage
    deleter.update_column_metadata("category", parent_name="status")

    result = deleter.delete_columns(["status"], use_transaction=False)

    assert result == {"status": False}
    columns_after = [
        row[0]
        for row in built_ducklake_schema.execute("DESCRIBE fact_table").fetchall()
    ]
    assert "status" in columns_after


# Test que cascade=True autorise la suppression et détache les enfants
def test_delete_columns_parent_with_cascade_detaches_children(
    deleter: DatabaseDeleter, built_ducklake_schema: Any
) -> None:
    """Test that cascade=True allows deleting a hierarchy parent and clears
    the children's parent_name.

    Args:
        deleter: DatabaseDeleter fixture.
        built_ducklake_schema: DuckDB connection.
    """
    # 'category' et 'status' sont déjà catégorielles (seuil=4) : aucun forçage
    deleter.update_column_metadata("category", parent_name="status")

    result = deleter.delete_columns(["status"], use_transaction=False, cascade=True)

    assert result == {"status": True}
    columns_after = [
        row[0]
        for row in built_ducklake_schema.execute("DESCRIBE fact_table").fetchall()
    ]
    assert "status" not in columns_after

    # La colonne enfant est toujours là, mais détachée de la hiérarchie
    parent_of_category = built_ducklake_schema.execute(
        "SELECT parent_name FROM metadata WHERE name = 'category'"
    ).fetchone()[0]
    assert parent_of_category is None


# ---------------------------------------------------------------------------
# Test de bout en bout de la compaction DuckLake après delete (§5.4-5.5)
# ---------------------------------------------------------------------------


# Test que delete_rows réussit avec compaction réelle sur un catalogue sur disque
@pytest.mark.skipif(
    not _ducklake_available(),
    reason="Extension ducklake non disponible dans cet environnement",
)
def test_delete_rows_compacts_on_real_ducklake_catalog(tmp_path: Any) -> None:
    """Test that delete_rows succeeds end-to-end against a real DuckLake catalog.

    Mirrors ``test_update_database_compacts_on_real_ducklake_catalog``: the
    in-memory fixture used elsewhere in this file can't exercise
    ``_run_ducklake_compaction`` for real.

    Args:
        tmp_path: pytest temporary directory.
    """
    catalog = str(tmp_path / "test.ducklake")
    data_dir = str(tmp_path / "data")
    os.makedirs(data_dir)
    conn = DuckLakeConnector(catalog, data_dir, data_inlining_row_limit=0).connect()

    df = pl.DataFrame(
        {
            "id": list(range(1, 6)),
            "category": ["A", "B", "A", "C", "B"],
            "value": [0.1, 0.2, 0.3, 0.4, 0.5],
        }
    )
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        DuckLakeTablesBuilder(
            df, categorical_threshold=4, primary_keys=["id"], connection=conn
        ).build_schema()

    deleter = DatabaseDeleter(connection=conn, categorical_threshold=4)
    deleted = deleter.delete_rows(filters=[("id", "=", 1)], use_transaction=False)

    assert deleted == 1
    row_count = conn.execute("SELECT COUNT(*) FROM fact_table").fetchone()[0]
    assert row_count == 4
    conn.close()

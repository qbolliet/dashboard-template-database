# Importation des modules
# Modules de base
from datetime import datetime
from typing import Any

import polars as pl

# Module de tests
# Modules du package à tester
from dt_ducklake_manager.operations import DatabaseUpdater

# ---------------------------------------------------------------------------
# Tests de l'initialisation
# ---------------------------------------------------------------------------


# Test de l'initialisation correcte de DatabaseUpdater
def test_updater_initialization(built_ducklake_schema: Any) -> None:
    """Test that DatabaseUpdater initializes without errors.

    Args:
        built_ducklake_schema: Fixture providing a DuckDB connection with a built
        schema.
    """
    updater = DatabaseUpdater(connection=built_ducklake_schema, categorical_threshold=4)
    assert updater is not None
    assert updater.categorical_threshold == 4
    assert updater.batch_size > 0


# Test de l'initialisation avec enable_validation=False
def test_updater_initialization_without_validation(built_ducklake_schema: Any) -> None:
    """Test that DatabaseUpdater can be initialized with validation disabled.

    Args:
        built_ducklake_schema: Fixture providing a DuckDB connection with a built
        schema.
    """
    updater = DatabaseUpdater(connection=built_ducklake_schema, enable_validation=False)
    assert updater.auditor is None


# Test que catalog_alias est propagé à tous les sous-gestionnaires
def test_updater_propagates_catalog_alias(built_ducklake_schema: Any) -> None:
    """Test that ``catalog_alias`` reaches every specialized sub-manager.

    Args:
        built_ducklake_schema: Fixture providing a DuckDB connection with a built
        schema.
    """
    updater = DatabaseUpdater(
        connection=built_ducklake_schema,
        catalog_alias="my_lake",
        schema="predictions",
    )
    assert updater.catalog_alias == "my_lake"
    assert updater.data_mgr.catalog_alias == "my_lake"
    assert updater.transaction_mgr.catalog_alias == "my_lake"
    assert updater.auditor is not None
    assert updater.auditor.catalog_alias == "my_lake"


# Test que catalog_alias vaut 'db' par défaut
def test_updater_default_catalog_alias(built_ducklake_schema: Any) -> None:
    """Test that ``catalog_alias`` defaults to 'db'.

    Args:
        built_ducklake_schema: Fixture providing a DuckDB connection with a built
        schema.
    """
    updater = DatabaseUpdater(connection=built_ducklake_schema)
    assert updater.catalog_alias == "db"
    assert updater.transaction_mgr.catalog_alias == "db"


# ---------------------------------------------------------------------------
# Tests de validate_operation()
# ---------------------------------------------------------------------------


# Test que validate_operation retourne True pour une insertion valide
def test_validate_operation_insert_returns_bool(
    updater: DatabaseUpdater, update_df: pl.DataFrame
) -> None:
    """Test that validate_operation returns a boolean for an insert operation.

    Args:
        updater: DatabaseUpdater fixture.
        update_df: DataFrame with new rows.
    """
    result = updater.validate_operation("insert", df=update_df)
    assert isinstance(result, bool)


# Test que validate_operation retourne True quand la validation est désactivée
def test_validate_operation_disabled_returns_true(
    built_ducklake_schema: Any, update_df: pl.DataFrame
) -> None:
    """Test that validate_operation always returns True when validation is disabled.

    Args:
        built_ducklake_schema: Fixture providing a DuckDB connection with a built
        schema.
        update_df: DataFrame with new rows.
    """
    updater = DatabaseUpdater(connection=built_ducklake_schema, enable_validation=False)
    result = updater.validate_operation("insert", df=update_df)
    assert result is True


# ---------------------------------------------------------------------------
# Tests de update_database()
# ---------------------------------------------------------------------------


# Test d'insertion de nouvelles lignes sans transaction
def test_update_database_insert_new_rows(
    updater: DatabaseUpdater, built_ducklake_schema: Any, update_df: pl.DataFrame
) -> None:
    """Test that update_database inserts new rows into the fact table.

    Args:
        updater: DatabaseUpdater fixture.
        built_ducklake_schema: DuckDB connection.
        update_df: DataFrame with new rows to insert.
    """
    # Comptage initial
    initial_count = built_ducklake_schema.execute(
        "SELECT COUNT(*) FROM fact_table"
    ).fetchone()[0]

    # Insertion des nouvelles lignes sans transaction
    # Remarque : keep='first' est requis car narwhals ne supporte pas keep=False (valeur
    # par défaut)
    result = updater.update_database(
        update_df=update_df,
        keep="first",
        use_transaction=False,
    )

    # Vérification que l'opération s'est bien déroulée
    assert isinstance(result, bool)
    assert result is True

    # Vérification que le nombre de lignes a augmenté
    final_count = built_ducklake_schema.execute(
        "SELECT COUNT(*) FROM fact_table"
    ).fetchone()[0]
    assert final_count > initial_count


# Test d'insertion avec déduplication sur les doublons du DataFrame d'entrée
def test_update_database_with_dedup_on_update(
    updater: DatabaseUpdater, built_ducklake_schema: Any
) -> None:
    """Test that update_database removes duplicates from the update
    DataFrame when requested.

    Args:
        updater: DatabaseUpdater fixture.
        built_ducklake_schema: DuckDB connection.
    """
    # DataFrame avec deux lignes IDENTIQUES sur toutes les colonnes (id=20, même date)
    # La déduplication utilise toutes les colonnes (primary_keys non transmises au
    # niveau du
    # DataFrame d'entrée) : les deux lignes doivent donc être parfaitement identiques.
    df_with_dup = pl.DataFrame(
        {
            "id": [20, 20, 21],
            "category": ["A", "A", "B"],
            "value": [5.0, 5.0, 6.0],
            "date": [datetime(2024, 3, 1), datetime(2024, 3, 1), datetime(2024, 3, 2)],
            "status": ["active", "active", "inactive"],
            "high_cardinality": ["val_300", "val_300", "val_301"],
        }
    )

    result = updater.update_database(
        update_df=df_with_dup,
        check_duplicates_update=True,
        check_duplicates_db=False,
        keep="first",
        use_transaction=False,
    )

    assert result is True

    # Vérification que la déduplication a fonctionné : id=20 ne doit apparaître qu'une
    # seule fois
    count_20 = built_ducklake_schema.execute(
        "SELECT COUNT(*) FROM fact_table WHERE id = 20"
    ).fetchone()[0]
    assert count_20 == 1

    # Vérification que id=21 a bien été inséré
    count_21 = built_ducklake_schema.execute(
        "SELECT COUNT(*) FROM fact_table WHERE id = 21"
    ).fetchone()[0]
    assert count_21 == 1


# ---------------------------------------------------------------------------
# Tests de changement de statut catégoriel lors d'une mise à jour
# ---------------------------------------------------------------------------


# Test de conversion non-catégorielle → catégorielle après remplacement de lignes
def test_update_database_non_categorical_becomes_categorical(
    updater: DatabaseUpdater, built_ducklake_schema: Any
) -> None:
    """Test that a non-categorical column becomes categorical when its
    unique value count drops to or below the threshold after an update.

    The sample schema is built with categorical_threshold=4.
    The column 'high_cardinality'
    initially has 5 unique values (val_100..val_104) and is therefore NOT categorical.
    After replacing all existing rows (id=1..5) with values from a set of only 3 unique
    labels, only the metadata flag should flip — the fact table is never rewritten.

    Args:
        updater: DatabaseUpdater fixture.
        built_ducklake_schema: DuckDB connection with the built schema.
    """
    # Vérification initiale :
    # high_cardinality n'est pas catégorielle (5 valeurs > seuil=4)
    is_cat_before = built_ducklake_schema.execute(
        "SELECT is_categorical FROM metadata WHERE name = 'high_cardinality'"
    ).fetchone()[0]
    assert is_cat_before is False

    # Remplacement de toutes les lignes existantes (id=1..5) via upsert :
    # les 5 nouvelles valeurs de high_cardinality
    # n'appartiennent qu'à 3 modalités distinctes :
    # (grp_A, grp_B, grp_C), ce qui est ≤ seuil=4
    # → conversion en variable catégorielle attendue.
    replacing_df = pl.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "category": ["A", "B", "A", "C", "B"],
            "value": [0.1, 0.2, 0.3, 0.4, 0.5],
            "date": pl.date_range(
                datetime(2024, 1, 1), datetime(2024, 1, 5), "1d", eager=True
            ),
            "status": ["active", "inactive", "active", "active", "inactive"],
            "high_cardinality": ["grp_A", "grp_B", "grp_C", "grp_A", "grp_B"],
        }
    )

    result = updater.update_database(
        update_df=replacing_df,
        keep="first",
        use_transaction=False,
    )

    assert result is True

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
    assert stored_labels == {"grp_A", "grp_B", "grp_C"}


# Test de conversion catégorielle → non-catégorielle après ajout de nouvelles modalités
def test_update_database_categorical_becomes_non_categorical(
    updater: DatabaseUpdater, built_ducklake_schema: Any
) -> None:
    """Test that a categorical column loses its categorical status when the number of
    distinct values exceeds the threshold after inserting new rows.

    The column 'category' starts with 3 unique values (A, B, C) and is categorical
    (threshold=4). After inserting rows that introduce 2 additional values (D, E),
    the fact table holds 5 modalities which exceeds the threshold, flipping the
    metadata flag to non-categorical.

    Args:
        updater: DatabaseUpdater fixture.
        built_ducklake_schema: DuckDB connection with the built schema.
    """
    # Vérification initiale : category est catégorielle
    is_cat_before = built_ducklake_schema.execute(
        "SELECT is_categorical FROM metadata WHERE name = 'category'"
    ).fetchone()[0]
    assert is_cat_before is True

    # Insertion de nouvelles lignes portant 2 modalités inédites pour category (D et E):
    # après insertion, la fact_table comptera [A, B, C, D, E] = 5 modalités > seuil=4
    # → bascule du seul booléen is_categorical attendue.
    expansion_df = pl.DataFrame(
        {
            "id": [10, 11, 12],
            "category": ["D", "E", "D"],
            "value": [1.0, 2.0, 3.0],
            "date": [datetime(2024, 3, 1), datetime(2024, 3, 2), datetime(2024, 3, 3)],
            "status": ["active", "inactive", "active"],
            "high_cardinality": ["val_200", "val_201", "val_202"],
        }
    )

    result = updater.update_database(
        update_df=expansion_df,
        keep="first",
        use_transaction=False,
    )

    assert result is True

    # Vérification : category n'est plus catégorielle dans les métadonnées
    is_cat_after = built_ducklake_schema.execute(
        "SELECT is_categorical FROM metadata WHERE name = 'category'"
    ).fetchone()[0]
    assert is_cat_after is False

    # Vérification : les libellés d'origine sont conservés, D et E compris
    stored_labels = {
        row[0]
        for row in built_ducklake_schema.execute(
            "SELECT DISTINCT category FROM fact_table"
        ).fetchall()
    }
    assert stored_labels == {"A", "B", "C", "D", "E"}


# ---------------------------------------------------------------------------
# Tests des valeurs manquantes dans les colonnes catégorielles lors d'un update
# ---------------------------------------------------------------------------


# Test que les NULL insérés via update_database restent NULL dans fact_table
def test_update_with_null_categorical_preserves_null_in_fact_table(
    updater: DatabaseUpdater, built_ducklake_schema: Any
) -> None:
    """Test that NULL values in categorical columns stay NULL after update_database.

    Inserts new rows with a NULL in the 'category' column alongside regular labels,
    and verifies that the NULL is stored as NULL — never as a placeholder — while
    the other rows keep their original labels.
    """
    # DataFrame d'insertion : un NULL en colonne catégorielle + des modalités connues
    df_with_nulls = pl.DataFrame(
        {
            "id": [30, 31, 32],
            "category": ["A", None, "B"],
            "value": [1.0, 2.0, 3.0],
            "date": [datetime(2024, 4, 1), datetime(2024, 4, 2), datetime(2024, 4, 3)],
            "status": ["active", "inactive", "active"],
            "high_cardinality": ["val_400", "val_401", "val_402"],
        }
    )

    result = updater.update_database(
        update_df=df_with_nulls,
        keep="first",
        use_transaction=False,
    )
    assert result is True

    # La ligne id=31 (category était NULL) doit toujours être NULL dans fact_table
    category_for_31 = built_ducklake_schema.execute(
        "SELECT category FROM fact_table WHERE id = 31"
    ).fetchone()[0]
    assert category_for_31 is None

    # Les lignes non nulles conservent leur libellé d'origine
    category_for_30 = built_ducklake_schema.execute(
        "SELECT category FROM fact_table WHERE id = 30"
    ).fetchone()[0]
    assert category_for_30 == "A"

    # Aucun placeholder ne doit apparaître parmi les modalités stockées
    stored_labels = {
        row[0]
        for row in built_ducklake_schema.execute(
            "SELECT DISTINCT category FROM fact_table WHERE category IS NOT NULL"
        ).fetchall()
    }
    assert "-1" not in stored_labels
    assert "nan" not in stored_labels


# Test qu'une colonne au statut forcé n'est jamais rebasculée par un update
def test_update_does_not_reflip_forced_categorical_column(
    updater: DatabaseUpdater, built_ducklake_schema: Any
) -> None:
    """Test that a forced categorical column keeps its status across an update.

    'high_cardinality' is forced to categorical even though it exceeds the
    threshold; an update must leave that decision untouched.
    """
    # Forçage du statut catégoriel, au-delà du seuil
    built_ducklake_schema.execute(
        "UPDATE metadata SET is_categorical = TRUE, is_categorical_forced = TRUE"
        " WHERE name = 'high_cardinality'"
    )
    updater._invalidate_metadata_cache()

    expansion_df = pl.DataFrame(
        {
            "id": [50, 51],
            "category": ["A", "B"],
            "value": [1.0, 2.0],
            "date": [datetime(2024, 6, 1), datetime(2024, 6, 2)],
            "status": ["active", "inactive"],
            "high_cardinality": ["val_600", "val_601"],
        }
    )

    assert (
        updater.update_database(
            update_df=expansion_df, keep="first", use_transaction=False
        )
        is True
    )

    # Le statut forcé est conservé malgré le dépassement du seuil
    is_cat_after = built_ducklake_schema.execute(
        "SELECT is_categorical FROM metadata WHERE name = 'high_cardinality'"
    ).fetchone()[0]
    assert is_cat_after is True


# Test que dataset_metadata.updated_at avance après un update réussi
def test_update_stamps_dataset_metadata(
    updater: DatabaseUpdater, built_ducklake_schema: Any
) -> None:
    """Test that a successful update refreshes dataset_metadata.updated_at."""
    # Horodatage volontairement ancien pour rendre l'avancée observable
    built_ducklake_schema.execute(
        "UPDATE dataset_metadata SET updated_at = TIMESTAMP '2000-01-01 00:00:00'"
    )
    before = built_ducklake_schema.execute(
        "SELECT updated_at FROM dataset_metadata"
    ).fetchone()[0]

    new_rows = pl.DataFrame(
        {
            "id": [60],
            "category": ["A"],
            "value": [1.0],
            "date": [datetime(2024, 7, 1)],
            "status": ["active"],
            "high_cardinality": ["val_700"],
        }
    )

    assert (
        updater.update_database(
            update_df=new_rows, keep="first", use_transaction=False
        )
        is True
    )

    after = built_ducklake_schema.execute(
        "SELECT updated_at FROM dataset_metadata"
    ).fetchone()[0]
    assert after > before

# Importation des modules
# Modules de base
import warnings
from typing import Any

import narwhals as nw

# Module de tests
import pytest

# Modules du package à tester
from dt_ducklake_manager.schema import SchemaBuilder

# ---------------------------------------------------------------------------
# Fixture locale
# ---------------------------------------------------------------------------


# Initialisation d'une instance de SchemaBuilder utilisée dans l'ensemble des tests
@pytest.fixture
def schema_builder(sample_df: Any) -> SchemaBuilder:
    """Initialize a SchemaBuilder instance with a sample DataFrame.

    Args:
        sample_df: polars DataFrame fixture from conftest.

    Returns:
        SchemaBuilder: initialized with categorical_threshold=4.
    """
    # Suppression du UserWarning lié à l'absence de clés primaires : ce comportement
    # est testé séparément dans test_warning_when_no_primary_keys.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        return SchemaBuilder(sample_df, categorical_threshold=4)


# ---------------------------------------------------------------------------
# Tests de l'initialisation
# ---------------------------------------------------------------------------


# Test de l'initialisation correcte des attributs de la classe
def test_schema_builder_initialization(schema_builder: Any, sample_df: Any) -> None:
    """Test the initialization of the SchemaBuilder class.

    Args:
        schema_builder: SchemaBuilder fixture.
        sample_df: Sample polars DataFrame.
    """
    # Vérification du type et de la bonne initialisation des attributs
    assert isinstance(schema_builder, SchemaBuilder)
    assert schema_builder.categorical_threshold == 4
    # Vérification que df est bien encapsulé dans narwhals
    assert isinstance(schema_builder.df, nw.DataFrame)
    # Vérification de l'équivalence de contenu (comparaison via les backends natifs)
    expected = nw.from_native(sample_df, eager_only=True)
    assert schema_builder.df.to_native().equals(expected.to_native())  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# Tests de create_metadata_table()
# ---------------------------------------------------------------------------


# Test de la création de la table des méta-données
def test_create_metadata_table(schema_builder: Any) -> None:
    """Test the build of the metadata table.

    Args:
        schema_builder: SchemaBuilder fixture.
    """
    # Création de la table des méta-données
    metadata = schema_builder.create_metadata_table()

    # Vérification du type renvoyé (narwhals DataFrame)
    assert isinstance(metadata, nw.DataFrame)
    # Vérification de l'existence de chacune des colonnes attendues
    assert "name" in metadata.columns
    assert "label" in metadata.columns
    assert "is_categorical_forced" in metadata.columns
    assert "sql_type" in metadata.columns
    assert "is_categorical" in metadata.columns
    assert "is_primary_key" in metadata.columns

    # Vérification de la bonne détection des variables catégorielles
    cat_filter = metadata.filter(nw.col("name") == "category")["is_categorical"][0]
    status_filter = metadata.filter(nw.col("name") == "status")["is_categorical"][0]
    high_card_filter = metadata.filter(nw.col("name") == "high_cardinality")[
        "is_categorical"
    ][0]
    assert cat_filter is True
    assert status_filter is True
    assert high_card_filter is False


# Test de l'ajout de labels personnalisés
# lors de la création de la table des méta-données
def test_create_metadata_table_with_labels(
    schema_builder: Any, column_labels: Any
) -> None:
    """Test the build of the metadata table with custom column labels.

    Args:
        schema_builder: SchemaBuilder fixture.
        column_labels: Dict mapping column names to custom labels.
    """
    # Création de la table des méta-données avec les labels
    metadata = schema_builder.create_metadata_table(column_labels)

    # Vérification de la bonne association des labels fournis
    for col, label in column_labels.items():
        row_label = metadata.filter(nw.col("name") == col)["label"][0]
        assert row_label == label


# ---------------------------------------------------------------------------
# Tests de categorical_overrides
# ---------------------------------------------------------------------------


# Test qu'une colonne peut être forcée catégorielle au-delà du seuil
def test_categorical_override_forces_true(sample_df: Any) -> None:
    """Test that a column above the threshold can be forced as categorical.

    'high_cardinality' has 5 modalities for a threshold of 4, so it would be
    inferred as non-categorical.

    Args:
        sample_df: Sample polars DataFrame.
    """
    builder = SchemaBuilder(
        sample_df,
        categorical_threshold=4,
        primary_keys=["id"],
        categorical_overrides={"high_cardinality": True},
    )
    metadata = builder.create_metadata_table()

    row = metadata.filter(nw.col("name") == "high_cardinality")
    assert row["is_categorical"][0] is True
    assert row["is_categorical_forced"][0] is True


# Test qu'une colonne sous le seuil peut être forcée non catégorielle
def test_categorical_override_forces_false(sample_df: Any) -> None:
    """Test that a column below the threshold can be forced as non-categorical.

    'category' has 3 modalities for a threshold of 4, so it would be inferred as
    categorical.

    Args:
        sample_df: Sample polars DataFrame.
    """
    builder = SchemaBuilder(
        sample_df,
        categorical_threshold=4,
        primary_keys=["id"],
        categorical_overrides={"category": False},
    )
    metadata = builder.create_metadata_table()

    row = metadata.filter(nw.col("name") == "category")
    assert row["is_categorical"][0] is False
    assert row["is_categorical_forced"][0] is True


# Test qu'une colonne non forcée n'est jamais marquée comme telle
def test_categorical_override_leaves_other_columns_unforced(sample_df: Any) -> None:
    """Test that columns absent from categorical_overrides stay threshold-driven.

    Args:
        sample_df: Sample polars DataFrame.
    """
    builder = SchemaBuilder(
        sample_df,
        categorical_threshold=4,
        primary_keys=["id"],
        categorical_overrides={"high_cardinality": True},
    )
    metadata = builder.create_metadata_table()

    row = metadata.filter(nw.col("name") == "category")
    assert row["is_categorical"][0] is True
    assert row["is_categorical_forced"][0] is False


# Test qu'une colonne inconnue dans categorical_overrides lève une ValueError
def test_categorical_override_unknown_column_raises(sample_df: Any) -> None:
    """Test that an unknown column in categorical_overrides raises a ValueError.

    Args:
        sample_df: Sample polars DataFrame.
    """
    with pytest.raises(ValueError, match="categorical_overrides"):
        SchemaBuilder(
            sample_df,
            categorical_threshold=4,
            primary_keys=["id"],
            categorical_overrides={"ghost_column": True},
        )


# ---------------------------------------------------------------------------
# Tests de create_fact_table()
# ---------------------------------------------------------------------------


# Test de la création de la table des faits
def test_create_fact_table(schema_builder: Any, sample_df: Any) -> None:
    """Test that the fact table holds the input values verbatim.

    Categorical columns keep their original labels: no synthetic code is ever
    substituted.

    Args:
        schema_builder: SchemaBuilder fixture.
        sample_df: Sample polars DataFrame.
    """
    # Création préalable des méta-données (prérequis)
    schema_builder.create_metadata_table()
    # Création de la table des faits
    fact_table = schema_builder.create_fact_table()

    # Vérification du type et de la longueur
    assert isinstance(fact_table, nw.DataFrame)
    assert len(fact_table) == len(sample_df)

    # Vérification que les colonnes catégorielles restent textuelles
    assert isinstance(fact_table.schema["category"], nw.String)
    assert isinstance(fact_table.schema["status"], nw.String)

    # Vérification que les libellés d'origine sont conservés tels quels
    assert fact_table["category"].to_list() == sample_df["category"].to_list()
    assert fact_table["status"].to_list() == sample_df["status"].to_list()


# ---------------------------------------------------------------------------
# Tests de build()
# ---------------------------------------------------------------------------


# Test de la construction complète du schéma
def test_build_complete_schema(schema_builder: Any, column_labels: Any) -> None:
    """Test the build of the complete schema (metadata and fact table).

    Args:
        schema_builder: SchemaBuilder fixture.
        column_labels: Dict mapping column names to custom labels.
    """
    # Construction du schéma complet
    metadata, fact_table = schema_builder.build(column_labels)

    # Vérification des types de chaque composant
    assert isinstance(metadata, nw.DataFrame)
    assert isinstance(fact_table, nw.DataFrame)
    # Une ligne de méta-données par colonne de la table des faits
    assert len(metadata) == len(fact_table.columns)


# ---------------------------------------------------------------------------
# Tests liés à categorical_threshold=None
# ---------------------------------------------------------------------------


# Test que categorical_threshold=None ne produit aucune colonne catégorielle
def test_categorical_threshold_none_no_categorical_columns(sample_df: Any) -> None:
    """Test that no column is marked categorical when categorical_threshold=None.

    Args:
        sample_df: Sample polars DataFrame.
    """
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        builder = SchemaBuilder(sample_df, categorical_threshold=None)

    metadata = builder.create_metadata_table()

    # Aucune colonne ne doit être marquée comme catégorielle
    assert not metadata["is_categorical"].to_list().__contains__(True)


# Test que categorical_threshold=None laisse la table des faits intacte
def test_categorical_threshold_none_keeps_labels(sample_df: Any) -> None:
    """Test that the fact table keeps its labels when categorical_threshold=None.

    The threshold only drives the UI flag; it never changes what is stored.

    Args:
        sample_df: Sample polars DataFrame.
    """
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        builder = SchemaBuilder(sample_df, categorical_threshold=None)

    _, fact_table = builder.build()

    # Les libellés d'origine sont conservés quel que soit le seuil
    assert fact_table["category"].to_list() == sample_df["category"].to_list()


# ---------------------------------------------------------------------------
# Tests liés aux clés primaires
# ---------------------------------------------------------------------------


# Test que UserWarning est levé quand aucune clé primaire n'est fournie
def test_warning_when_no_primary_keys(sample_df: Any) -> None:
    """Test that a UserWarning is raised when no primary keys are specified.

    Args:
        sample_df: Sample polars DataFrame.
    """
    with pytest.warns(UserWarning, match="No primary key"):
        SchemaBuilder(sample_df, categorical_threshold=4)


# Test qu'aucun avertissement n'est levé quand des clés primaires sont fournies
def test_no_warning_when_primary_keys_provided(sample_df: Any) -> None:
    """Test that no UserWarning is raised when primary_keys is provided.

    Args:
        sample_df: Sample polars DataFrame.
    """
    # Traitement des warnings comme des erreurs
    # pour détecter toute émission non attendue
    with warnings.catch_warnings():
        warnings.simplefilter("error", UserWarning)
        # Ne doit pas lever d'exception
        SchemaBuilder(sample_df, categorical_threshold=4, primary_keys=["id"])


# Test que les clés primaires sont marquées dans les méta-données
def test_primary_keys_marked_in_metadata(sample_df: Any) -> None:
    """Test that primary key columns are marked as is_primary_key=True in metadata.

    Args:
        sample_df: Sample polars DataFrame.
    """
    builder = SchemaBuilder(sample_df, categorical_threshold=4, primary_keys=["id"])
    metadata = builder.create_metadata_table()

    # Vérification que la colonne 'id' est marquée comme clé primaire
    id_pk = metadata.filter(nw.col("name") == "id")["is_primary_key"][0]
    assert id_pk is True

    # Vérification que les autres colonnes ne sont pas marquées comme clés primaires
    value_pk = metadata.filter(nw.col("name") == "value")["is_primary_key"][0]
    assert value_pk is False


# Test qu'une ValueError est levée pour une clé primaire inexistante
def test_invalid_primary_key_raises_value_error(sample_df: Any) -> None:
    """Test that a ValueError is raised when a primary key column does not exist.

    Args:
        sample_df: Sample polars DataFrame.
    """
    with pytest.raises(ValueError, match="nonexistent_col"):
        SchemaBuilder(sample_df, primary_keys=["nonexistent_col"])


# ---------------------------------------------------------------------------
# Tests des valeurs manquantes dans les colonnes catégorielles
# ---------------------------------------------------------------------------


# Invariant : un NULL/None dans une colonne catégorielle doit rester NULL dans la
# fact_table, et les autres positions conserver leur libellé d'origine.
def test_null_in_categorical_column_preserved_in_fact_table() -> None:
    """Test that NULL values in a categorical column stay NULL in the fact table.

    The non-null positions must keep their original labels verbatim.
    """
    import polars as pl

    df_with_nulls = pl.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "category": ["A", None, "B", "A", None],
            "value": [0.1, 0.2, 0.3, 0.4, 0.5],
        }
    )

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        builder = SchemaBuilder(
            df_with_nulls, categorical_threshold=4, primary_keys=["id"]
        )

    metadata, fact_table = builder.build()

    # La colonne 'category' doit être signalée comme catégorielle
    is_cat = metadata.filter(nw.col("name") == "category")["is_categorical"][0]
    assert is_cat is True

    # La fact_table conserve les NULL aux positions originales (lignes id=2 et id=5)
    category_col = fact_table["category"].to_list()
    # Les indices 1 et 4 (id=2 et id=5) doivent rester NULL/None
    assert category_col[1] is None
    assert category_col[4] is None
    # Les autres positions conservent leur libellé d'origine
    assert category_col[0] == "A"
    assert category_col[2] == "B"
    assert category_col[3] == "A"


# Test que tous les NULL d'une colonne catégorielle restent NULL dans la fact_table
def test_all_null_rows_preserved_as_null_in_fact_table() -> None:
    """Test that every NULL row in a categorical column stays NULL in fact_table.

    The number of NULL values in the categorical column of the fact_table must
    equal the number of NULL values in the source DataFrame.
    """
    import polars as pl

    df_with_nulls = pl.DataFrame(
        {
            "id": [1, 2, 3, 4, 5, 6],
            "status": ["active", None, "inactive", None, "active", None],
            "value": [0.1, 0.2, 0.3, 0.4, 0.5, 0.6],
        }
    )

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        builder = SchemaBuilder(
            df_with_nulls, categorical_threshold=4, primary_keys=["id"]
        )

    _, fact_table = builder.build()

    # Comptage des NULL dans la fact_table : doit correspondre au DataFrame source
    status_values = fact_table["status"].to_list()
    status_nulls = sum(1 for v in status_values if v is None)
    assert status_nulls == 3

    # Les positions non nulles conservent leur libellé d'origine
    assert status_values[0] == "active"
    assert status_values[2] == "inactive"

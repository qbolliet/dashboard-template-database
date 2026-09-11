# Importation des modules
import narwhals as nw

# Champs d'UI de la table metadata renseignés par le producteur de métadonnées.
# Tous VARCHAR nullable (NULL par défaut) ; un update de données ne les écrase jamais.
# parent_name porte la hiérarchie de colonnes : la colonne
# parente dans un menu à group-options / arbre de sélection.
UI_METADATA_FIELDS: tuple[str, ...] = (
    "parent_name",
    "unit",
    "display_format",
    "family",
    "description",
    "default_aggregation",
)

# Clés acceptées dans un sous-dictionnaire de ``column_metadata`` : les champs d'UI
# ci-dessus, plus le libellé d'affichage.
COLUMN_METADATA_KEYS: frozenset[str] = frozenset({"label", *UI_METADATA_FIELDS})

# Agrégations acceptées pour ``metadata.default_aggregation``, validées à l'écriture.
ALLOWED_DEFAULT_AGGREGATIONS: frozenset[str] = frozenset(
    {"SUM", "AVG", "MAX", "MIN", "COUNT", "MEDIAN", "MODE"}
)


# Fonction de normalisation et de validation de ``metadata.default_aggregation``
def normalize_default_aggregation(value: str | None) -> str | None:
    """
    Normalize and validate a ``default_aggregation`` value.

    ``None`` passes through unchanged (the field is nullable). A string is
    upper-cased and checked against :data:`ALLOWED_DEFAULT_AGGREGATIONS`.

    Args:
        value (str | None): Raw aggregation label supplied by the metadata
            producer.

    Returns:
        str | None: The upper-cased aggregation label, or ``None``.

    Raises:
        ValueError: If ``value`` is neither ``None`` nor one of the allowed
            aggregations (case-insensitive).

    Examples:
        >>> normalize_default_aggregation('sum')
        'SUM'
        >>> normalize_default_aggregation(None) is None
        True
        >>> normalize_default_aggregation('total')  # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
            ...
        ValueError: Invalid default_aggregation 'total'; ...
    """
    # Champ nullable : absence de valeur acceptée telle quelle
    if value is None:
        return None

    # Normalisation en majuscules puis contrôle d'appartenance à la liste blanche
    normalized = str(value).upper()
    if normalized not in ALLOWED_DEFAULT_AGGREGATIONS:
        raise ValueError(
            f"Invalid default_aggregation {value!r}; expected one of "
            f"{sorted(ALLOWED_DEFAULT_AGGREGATIONS)} or None"
        )
    return normalized


# Hiérarchie des types SQL : (niveau, largeur en bits).
# Le niveau ordonne les familles (booléen < entier < flottant < texte) ; la largeur
# ordonne les types d'un même niveau, garantissant qu'un BIGINT enregistré n'est
# jamais rétrogradé par un lot d'Int32.
# Les types non ordonnés (DECIMAL, DATE, TIMESTAMP, TIME, INTERVAL, BLOB) sont
# volontairement absents : aucun élargissement n'a de sens entre eux et les types
# ci-dessous, et les y rattacher promouvrait à tort une colonne temporelle en VARCHAR.
SQL_TYPE_RANK: dict[str, tuple[int, int]] = {
    # Niveau 1 : booléen
    "BOOLEAN": (1, 1),
    # Niveau 2 : entiers, ordonnés par largeur
    "TINYINT": (2, 8),
    "UTINYINT": (2, 8),
    "SMALLINT": (2, 16),
    "USMALLINT": (2, 16),
    "INTEGER": (2, 32),
    "UINTEGER": (2, 32),
    "BIGINT": (2, 64),
    "UBIGINT": (2, 64),
    "HUGEINT": (2, 128),
    "UHUGEINT": (2, 128),
    # Niveau 3 : virgule flottante
    "FLOAT": (3, 32),
    "DOUBLE": (3, 64),
    # Niveau 4 : texte
    "VARCHAR": (4, 0),
}

# Types entiers non signés : à largeur égale, ils ne contiennent pas leur homologue
# signé (INTEGER et UINTEGER se recouvrent partiellement seulement).
UNSIGNED_SQL_TYPES: frozenset[str] = frozenset(
    {"UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT", "UHUGEINT"}
)

# Promotion en cas de conflit de signe : plus petit type signé contenant à la fois
# le type signé et le type non signé de la largeur indiquée.
SIGNED_WIDENING: dict[int, str] = {
    8: "SMALLINT",
    16: "INTEGER",
    32: "BIGINT",
    64: "HUGEINT",
}


# Fonction associant les types narwhals à leur équivalent SQL
def map_python_to_sql_type(dtype: nw.dtypes.DType) -> str:
    """
    Map Narwhals data types to SQL-compatible data types.

    Integer and float widths are preserved: the package never silently narrows a
    column. It is up to the producer to supply a ``Float32`` (or a narrower integer)
    when 32 bits are deemed sufficient.

    Args:
        dtype (nw.DType): The Narwhals data type.

    Returns:
        str: The corresponding SQL data type.

    Examples:
        >>> import narwhals as nw
        >>> import polars as pl
        >>> df = pl.DataFrame({'col': ['a', 'b']})
        >>> map_python_to_sql_type(df.schema['col'])
        'VARCHAR'
        >>> df = pl.DataFrame({'col': [1, 2]})  # polars infère Int64
        >>> map_python_to_sql_type(df.schema['col'])
        'BIGINT'
        >>> df = pl.DataFrame({'col': [1.0, 2.0]})  # polars infère Float64
        >>> map_python_to_sql_type(df.schema['col'])
        'DOUBLE'
    """
    # Types textuels
    # String, Categorical et Enum sont tous stockés sous forme VARCHAR en SQL
    if isinstance(dtype, (nw.String, nw.Categorical, nw.Enum)):
        return "VARCHAR"

    # Entiers signés
    # Préservation de la largeur : chaque type narwhals conserve son type SQL dédié.
    # Int128 est mappé vers HUGEINT, le type entier 128 bits natif de DuckDB.
    elif isinstance(dtype, nw.Int8):
        return "TINYINT"
    elif isinstance(dtype, nw.Int16):
        return "SMALLINT"
    elif isinstance(dtype, nw.Int32):
        return "INTEGER"
    elif isinstance(dtype, nw.Int64):
        return "BIGINT"
    elif isinstance(dtype, nw.Int128):
        return "HUGEINT"

    # Entiers non signés
    # Chaque largeur de bit possède un type UNSIGNED dédié dans DuckDB
    elif isinstance(dtype, nw.UInt8):
        return "UTINYINT"
    elif isinstance(dtype, nw.UInt16):
        return "USMALLINT"
    elif isinstance(dtype, nw.UInt32):
        return "UINTEGER"
    elif isinstance(dtype, nw.UInt64):
        return "UBIGINT"
    elif isinstance(dtype, nw.UInt128):
        return "UHUGEINT"

    # Types virgule flottante
    # Préservation de la largeur : Float32 → FLOAT (32 bits), Float64 → DOUBLE (64 bits)
    elif isinstance(dtype, nw.Float32):
        return "FLOAT"
    elif isinstance(dtype, nw.Float64):
        return "DOUBLE"

    # Type décimal à précision fixe
    # On retourne DECIMAL sans précision ni échelle car ces paramètres ne sont
    # pas toujours disponibles au moment de la construction du schéma SQL.
    elif isinstance(dtype, nw.Decimal):
        return "DECIMAL"

    # Types temporels
    elif isinstance(dtype, nw.Date):
        return "DATE"
    elif isinstance(dtype, nw.Datetime):
        return "TIMESTAMP"
    elif isinstance(dtype, nw.Duration):
        return "INTERVAL"
    elif isinstance(dtype, nw.Time):
        return "TIME"

    # Type booléen
    elif isinstance(dtype, nw.Boolean):
        return "BOOLEAN"

    # Type binaire
    # BLOB est le type DuckDB pour les données binaires brutes
    elif isinstance(dtype, nw.Binary):
        return "BLOB"

    # Types composites (Array, List, Struct)
    # DuckDB supporte nativement ces types, mais leur définition SQL complète
    # nécessiterait la connaissance des types imbriqués. On replie vers VARCHAR
    # pour garantir la compatibilité dans tous les contextes d'usage.
    elif isinstance(dtype, (nw.Array, nw.List, nw.Struct)):
        return "VARCHAR"

    # Cas de repli : Object, Unknown, et tout type non reconnu
    else:
        return "VARCHAR"


# Fonction de résolution d'un conflit de types SQL entre la base et un lot entrant
def resolve_sql_type_conflict(current: str, new: str) -> str | None:
    """
    Return the SQL type to store when a batch type differs from the stored one.

    Widening only: the recorded type is never narrowed. Within a level the greater
    width wins, so a stored ``BIGINT`` survives a batch of ``Int32``. At equal
    width, a signed/unsigned clash is promoted to the smallest signed type holding
    both. Non-ordered types (``DECIMAL``, ``DATE``, ``TIMESTAMP``, ``TIME``,
    ``INTERVAL``, ``BLOB``) have no ordering and always keep the stored type.

    Args:
        current (str): SQL type currently recorded in ``metadata.sql_type``.
        new (str): SQL type inferred from the incoming batch.

    Returns:
        str | None: The SQL type to write, or ``None`` when the stored type must
        be kept.

    Examples:
        >>> resolve_sql_type_conflict('BIGINT', 'INTEGER') is None
        True
        >>> resolve_sql_type_conflict('INTEGER', 'BIGINT')
        'BIGINT'
        >>> resolve_sql_type_conflict('INTEGER', 'UINTEGER')
        'BIGINT'
        >>> resolve_sql_type_conflict('INTEGER', 'VARCHAR')
        'VARCHAR'
        >>> resolve_sql_type_conflict('TIMESTAMP', 'BIGINT') is None
        True
    """
    # Types identiques : aucun conflit à résoudre
    if current == new:
        return None

    # Rangs respectifs des deux types
    current_rank = SQL_TYPE_RANK.get(current)
    new_rank = SQL_TYPE_RANK.get(new)

    # Type non ordonné d'un côté ou de l'autre : conservation du type enregistré
    if current_rank is None or new_rank is None:
        return None

    # Comparaison lexicographique du couple (niveau, largeur)
    if new_rank > current_rank:
        return new
    if new_rank < current_rank:
        return None

    # Niveau et largeur identiques : conflit de signe, promotion au type signé
    # immédiatement supérieur (None en 128 bits, aucun entier plus large n'existant)
    if (current in UNSIGNED_SQL_TYPES) != (new in UNSIGNED_SQL_TYPES):
        return SIGNED_WIDENING.get(current_rank[1])

    return None

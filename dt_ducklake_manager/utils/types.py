# Importation des modules
import narwhals as nw


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

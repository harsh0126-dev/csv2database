import pandas as pd

DTYPE_MAP = {
    "int64": "INTEGER",
    "float64": "FLOAT",
    "bool": "BOOLEAN",
    "datetime64[ns]": "TIMESTAMP",
    "object": "TEXT",
}


def pandas_dtype_to_sql(dtype):
    return DTYPE_MAP.get(str(dtype), "TEXT")


# ===================================================================
# 📌 Detect potential primary key (NOT final)
# ===================================================================
def detect_primary_key(df: pd.DataFrame):
    candidates = []
    num_rows = len(df)

    for col in df.columns:
        s = df[col]
        score = 0

        if s.nunique(dropna=True) == num_rows:
            score += 10

        if s.isnull().sum() == 0:
            score += 3

        if "id" in col.lower():
            score += 5

        if str(s.dtype).startswith(("int", "float")):
            score += 2

        if score > 0:
            candidates.append((col, score))

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[1], reverse=True)
    return candidates[0][0]


# ===================================================================
# 📌 Validate PK before applying
# ===================================================================
def is_valid_primary_key(df: pd.DataFrame, col: str):
    if col is None:
        return False

    s = df[col]

    # No duplicates
    if s.nunique(dropna=True) != len(s):
        return False

    # No nulls
    if s.isnull().any():
        return False

    return True


# ===================================================================
# 📌 Build CREATE TABLE SQL
# ===================================================================
def build_create_table_sql(table_name, df, pk=None):
    cols = []

    for col, dtype in df.dtypes.items():
        safe = col.replace('"', "")
        sql_type = pandas_dtype_to_sql(dtype)

        line = f'"{safe}" {sql_type}'
        if pk and safe == pk:
            line += " PRIMARY KEY"

        cols.append(line)

    return f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(cols)});'

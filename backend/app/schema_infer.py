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


# ----------------------------------------------------------
# 🔥 PRIMARY KEY DETECTION
# ----------------------------------------------------------
def detect_primary_key(df: pd.DataFrame):
    num_rows = len(df)
    candidates = []

    for col in df.columns:
        series = df[col]
        score = 0

        if series.nunique(dropna=True) == num_rows:
            score += 10

        if series.isnull().sum() == 0:
            score += 3

        if "id" in col.lower().replace(" ", ""):
            score += 5

        if str(series.dtype).startswith(("int", "float")):
            score += 2

        if score > 0:
            candidates.append((col, score))

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[1], reverse=True)
    return candidates[0][0]


# ----------------------------------------------------------
# 🔥 FOREIGN KEY DETECTION
# ----------------------------------------------------------
def detect_foreign_keys(tables, pk_map):
    fk_relations = []

    for child_table, child_df in tables.items():
        for child_col in child_df.columns:

            child_series = child_df[child_col]

            for parent_table, parent_df in tables.items():
                parent_pk = pk_map[parent_table]

                if child_table == parent_table:
                    continue

                score = 0

                # name similarity
                if parent_pk.lower() in child_col.lower():
                    score += 5

                # % match instead of full match
                parent_values = parent_df[parent_pk].dropna().astype(str)
                child_values = child_series.dropna().astype(str)

                match_ratio = child_values.isin(parent_values).mean()

                if match_ratio >= 0.70:   # 70% of rows match
                    score += 10

                # repeating values in child = typical FK property
                if child_series.nunique() < len(child_series):
                    score += 2

                if score >= 10:
                    fk_relations.append({
                        "child_table": child_table,
                        "child_column": child_col,
                        "parent_table": parent_table,
                        "parent_pk": parent_pk
                    })

    return fk_relations



# ----------------------------------------------------------
# 🔥 SQL BUILDER
# ----------------------------------------------------------
def build_create_table_sql(table_name, df, pk=None):
    cols = []

    for col, dtype in df.dtypes.items():
        safe_col = col.replace('"', '').replace(" ", "_")
        sql_type = pandas_dtype_to_sql(dtype)
        stmt = f'"{safe_col}" {sql_type}'
        if pk and safe_col == pk:
            stmt += " PRIMARY KEY"
        cols.append(stmt)

    return f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(cols)});'

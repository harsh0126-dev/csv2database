from io import BytesIO
import pandas as pd
from pathlib import Path
from sqlalchemy import text

from .schema_infer import (
    detect_primary_key,
    detect_foreign_keys,
    build_create_table_sql
)
from .db import engine

UPLOAD_DIR = Path.cwd() / "uploads"
UPLOAD_DIR.mkdir(exist_ok=True)


def load_csv_node(file_bytes: bytes, filename: str):
    saved = UPLOAD_DIR / filename
    saved.write_bytes(file_bytes)

    try:
        df = pd.read_csv(BytesIO(file_bytes))
    except:
        df = pd.read_csv(BytesIO(file_bytes), encoding="latin1")

    df.columns = [c.replace(" ", "_").replace(".", "_") for c in df.columns]

    return {
        "filename": filename,
        "df": df,
        "saved_path": str(saved)
    }


def relationship_node(state: dict):
    tables = state["tables"]

    pk_map = {}
    for name, df in tables.items():
        pk_map[name] = detect_primary_key(df)

    fk_relations = detect_foreign_keys(tables, pk_map)

    state["pk_map"] = pk_map
    state["fk_relations"] = fk_relations
    return state


def db_write_node(state: dict):
    tables = state["tables"]
    pk_map = state["pk_map"]
    fk_relations = state["fk_relations"]

    with engine.begin() as conn:

        # STEP 1: Create ALL tables WITHOUT FKs
        for table_name, df in tables.items():
            create_sql = build_create_table_sql(
                table_name, df, pk=pk_map[table_name]
            )
            conn.execute(text(create_sql))

        # STEP 2: Insert Data
        for table_name, df in tables.items():
            df.to_sql(table_name, conn, if_exists="append", index=False)

        # STEP 3: Add FK Constraints AFTER insert
        for fk in fk_relations:
            fk_sql = f'''
            ALTER TABLE "{fk['child_table']}"
            ADD CONSTRAINT fk_{fk["child_table"]}_{fk["child_column"]}
            FOREIGN KEY ("{fk["child_column"]}")
            REFERENCES "{fk["parent_table"]}"("{fk["parent_pk"]}");
            '''
            conn.execute(text(fk_sql))

    return state

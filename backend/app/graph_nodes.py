# graph_nodes.py

from io import BytesIO
from pathlib import Path
import pandas as pd
import requests
from sqlalchemy import text, inspect

from .schema_infer import (
    detect_primary_key,
    is_valid_primary_key,
    build_create_table_sql,
)
from .db import engine

UPLOAD_DIR = Path.cwd() / "uploads"
UPLOAD_DIR.mkdir(exist_ok=True)


# ===================================================================
# 📌 Helper: Load Google Sheet from URL
# ===================================================================
def load_google_sheet(url: str):
    url = url.strip()

    # Validate URL
    if not (url.startswith("http") and "docs.google.com" in url):
        raise ValueError("Invalid Google Sheet URL.")

    # Convert to CSV export URL
    if "/edit" in url:
        csv_url = url.split("/edit")[0] + "/export?format=csv"
    else:
        csv_url = url + "/export?format=csv"

    resp = requests.get(csv_url, verify=False)
    if resp.status_code != 200:
        raise ValueError("Google Sheet could not be loaded. "
                         "Make sure it is PUBLIC (Anyone with link → Viewer).")

    return pd.read_csv(BytesIO(resp.content))


# ===================================================================
# 📌 1. FILE LOADER (CSV + Excel + Google Sheet)
# ===================================================================
def load_file_node(file_bytes: bytes, filename: str):
    """
    Strict loader for CSV + Excel ONLY.
    Google Sheets must use /upload-sheet endpoint.
    """
    print("\n📌 RECEIVED FILENAME:", filename, "\n")

    saved = UPLOAD_DIR / filename
    saved.write_bytes(file_bytes)

    ext = filename.split(".")[-1].lower()

    # CSV
    if ext == "csv":
        try:
            df = pd.read_csv(BytesIO(file_bytes))
        except:
            df = pd.read_csv(BytesIO(file_bytes), encoding="latin1")

    # Excel
    elif ext in ("xls", "xlsx"):
        df = pd.read_excel(BytesIO(file_bytes))

    # If user tries Google Sheet here → reject
    elif ext == "gsheet" or "docs.google.com" in filename:
        raise ValueError(
            "Google Sheets must be uploaded using /upload-sheet endpoint only."
        )

    else:
        raise ValueError("Unsupported file type. Use CSV or Excel only.")

    # Clean columns
    df.columns = [c.replace(" ", "_").replace(".", "_") for c in df.columns]

    return {
        "filename": filename,
        "df": df,
        "saved_path": str(saved),
    }





# ===================================================================
# 📌 2. Detect primary key (optional)
# ===================================================================
def relationship_node(state: dict):
    tables = state["tables"]
    pk_map = {}

    for table_name, df in tables.items():
        detected_pk = detect_primary_key(df)

        # Apply PK only if clean/valid
        if is_valid_primary_key(df, detected_pk):
            pk_map[table_name] = detected_pk
        else:
            pk_map[table_name] = None  # save normal table without PK

    state["pk_map"] = pk_map
    return state


# ===================================================================
# 📌 3. Write to DB (create table + append data)
# ===================================================================
def db_write_node(state: dict):
    tables = state["tables"]
    pk_map = state["pk_map"]

    inspector = inspect(engine)
    existing_tables = inspector.get_table_names()

    with engine.begin() as conn:

        # Create tables if missing
        for table_name, df in tables.items():

            pk = pk_map.get(table_name)  # may be None

            if table_name not in existing_tables:
                sql = build_create_table_sql(table_name, df, pk)
                conn.execute(text(sql))

        # Append data ALWAYS
        for table_name, df in tables.items():
            df.to_sql(table_name, conn, if_exists="append", index=False)

    return state

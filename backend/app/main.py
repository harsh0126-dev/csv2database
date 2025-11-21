from fastapi import FastAPI, UploadFile, File, Form, HTTPException
import traceback

from .graph_nodes import (
    load_file_node,
    load_google_sheet,
    relationship_node,
    db_write_node,
)

app = FastAPI(title="CSV / Excel / Google Sheet → Database")


# ===============================================
# 1. CSV / Excel Upload Multiple
# ===============================================
@app.post("/upload-multiple")
async def upload_multiple(files: list[UploadFile] = File(...)):
    try:
        tables = {}

        for f in files:
            content = await f.read()
            result = load_file_node(content, f.filename)
            table_name = f.filename.split(".")[0].lower()
            tables[table_name] = result["df"]

        state = {"tables": tables}
        state = relationship_node(state)
        state = db_write_node(state)

        return {
            "tables": list(tables.keys()),
            "pk_map": state["pk_map"],
        }

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


# ===============================================
# 2. CLEAN Google Sheet API
# ===============================================
@app.post("/upload-sheet")
async def upload_sheet(sheet_url: str = Form(...), table_name: str = Form(...)):
    try:
        df = load_google_sheet(sheet_url)
        df.columns = [c.replace(" ", "_").replace(".", "_") for c in df.columns]

        # PK detection
        from .schema_infer import detect_primary_key, is_valid_primary_key
        pk = detect_primary_key(df)
        if not is_valid_primary_key(df, pk):
            pk = None

        state = {"tables": {table_name: df}, "pk_map": {table_name: pk}}
        db_write_node(state)

        return {
            "message": "Google Sheet uploaded successfully",
            "table": table_name,
            "primary_key": pk,
        }

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

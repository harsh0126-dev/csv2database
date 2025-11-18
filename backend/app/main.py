import logging
import traceback
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse

from .graph_nodes import (
    load_csv_node,
    relationship_node,
    db_write_node
)

logging.basicConfig(level=logging.DEBUG)

app = FastAPI(title="CSV → SQL Auto Modeler")


@app.post("/upload-multiple")
async def upload_multiple(files: list[UploadFile] = File(...)):
    try:
        tables = {}

        # Load all CSVs
        for f in files:
            content = await f.read()
            result = load_csv_node(content, f.filename)
            table_name = f.filename.split(".")[0].lower()
            tables[table_name] = result["df"]

        state = {"tables": tables}

        # PK + FK detection
        state = relationship_node(state)

        # Create tables + insert data + add FKs
        state = db_write_node(state)

        return {
            "primary_keys": state["pk_map"],
            "foreign_keys": state["fk_relations"],
            "tables_created": list(tables.keys())
        }

    except Exception as e:
        traceback.print_exc()  # <──── REAL ERROR PRINTED HERE
        raise HTTPException(status_code=500, detail=str(e))

import os
import logging
from fastapi import FastAPI, HTTPException, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from backend.lineage_service import (
    list_catalogs,
    list_schemas,
    get_table_lineage,
    get_column_lineage,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Lineage Explorer", version="1.0.0")


@app.get("/api/catalogs")
def api_list_catalogs():
    try:
        return {"catalogs": list_catalogs()}
    except Exception as e:
        logger.error(f"Error listing catalogs: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/schemas")
def api_list_schemas(catalog: str = Query(...)):
    try:
        return {"schemas": list_schemas(catalog)}
    except Exception as e:
        logger.error(f"Error listing schemas: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/lineage")
def api_get_lineage(catalog: str = Query(...), schema: str = Query(...)):
    try:
        return get_table_lineage(catalog, schema)
    except Exception as e:
        logger.error(f"Error getting lineage: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/column-lineage")
def api_get_column_lineage(
    catalog: str = Query(...),
    schema: str = Query(...),
    table: str = Query(...),
    column: str = Query(...),
):
    try:
        return get_column_lineage(catalog, schema, table, column)
    except Exception as e:
        logger.error(f"Error getting column lineage: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Serve frontend static files
static_dir = os.path.join(os.path.dirname(__file__), "..", "frontend", "dist")
if os.path.exists(static_dir):
    app.mount("/assets", StaticFiles(directory=os.path.join(static_dir, "assets")), name="assets")

    @app.get("/{full_path:path}")
    def serve_frontend(full_path: str):
        file_path = os.path.join(static_dir, full_path)
        if os.path.isfile(file_path):
            return FileResponse(file_path)
        return FileResponse(os.path.join(static_dir, "index.html"))

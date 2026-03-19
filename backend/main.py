import os
import time
import asyncio
import logging
from collections import defaultdict
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from backend.lineage_service import (
    list_catalogs,
    list_schemas,
    get_table_lineage,
    get_column_lineage,
    get_columns,
    invalidate_cache,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Lineage Explorer", version="1.1.0")


# ---------------------------------------------------------------------------
# Rate limiting middleware — protects DBSQL warehouse from abuse
# ---------------------------------------------------------------------------
class RateLimitMiddleware(BaseHTTPMiddleware):
    """Simple in-memory rate limiter: max requests per IP per window."""

    def __init__(self, app, max_requests: int = 60, window_seconds: int = 60):
        super().__init__(app)
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.requests: dict[str, list[float]] = defaultdict(list)

    async def dispatch(self, request: Request, call_next):
        # Only rate-limit API endpoints
        if not request.url.path.startswith("/api/"):
            return await call_next(request)

        client_ip = request.client.host if request.client else "unknown"
        now = time.time()
        # Prune old entries
        self.requests[client_ip] = [
            t for t in self.requests[client_ip]
            if now - t < self.window_seconds
        ]
        if len(self.requests[client_ip]) >= self.max_requests:
            return JSONResponse(
                status_code=429,
                content={"detail": "Rate limit exceeded. Try again shortly."},
            )
        self.requests[client_ip].append(now)
        return await call_next(request)


app.add_middleware(RateLimitMiddleware, max_requests=60, window_seconds=60)


# ---------------------------------------------------------------------------
# API endpoints — async wrappers around synchronous SDK calls
# ---------------------------------------------------------------------------

@app.get("/api/catalogs")
async def api_list_catalogs():
    try:
        result = await asyncio.to_thread(list_catalogs)
        return {"catalogs": result}
    except Exception as e:
        logger.error(f"Error listing catalogs: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/schemas")
async def api_list_schemas(catalog: str = Query(...)):
    try:
        result = await asyncio.to_thread(list_schemas, catalog)
        return {"schemas": result}
    except Exception as e:
        logger.error(f"Error listing schemas: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/lineage")
async def api_get_lineage(catalog: str = Query(...), schema: str = Query(...), live: bool = Query(False)):
    try:
        if live:
            logger.info(f"LIVE MODE: Serving lineage for {catalog}.{schema} direct from system tables")
        return await asyncio.to_thread(get_table_lineage, catalog, schema, live)
    except Exception as e:
        logger.error(f"Error getting lineage: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/columns")
async def api_get_columns(
    catalog: str = Query(...),
    schema: str = Query(...),
    table: str = Query(...),
    live: bool = Query(False),
):
    """Lazy column loader — fetch columns for a single table on demand."""
    try:
        cols = await asyncio.to_thread(get_columns, catalog, schema, table, live)
        return {"columns": cols}
    except Exception as e:
        logger.error(f"Error getting columns: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/column-lineage")
async def api_get_column_lineage(
    catalog: str = Query(...),
    schema: str = Query(...),
    table: str = Query(...),
    column: str = Query(...),
    live: bool = Query(False),
):
    try:
        return await asyncio.to_thread(get_column_lineage, catalog, schema, table, column)
    except Exception as e:
        logger.error(f"Error getting column lineage: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/cache/invalidate")
async def api_invalidate_cache():
    """Admin endpoint to force-clear all cached data."""
    invalidate_cache()
    return {"status": "ok", "message": "Cache cleared"}


# Serve frontend static files
static_dir = os.path.join(os.path.dirname(__file__), "..", "frontend", "dist")
if os.path.exists(static_dir):
    app.mount("/assets", StaticFiles(directory=os.path.join(static_dir, "assets")), name="assets")

    @app.get("/{full_path:path}")
    async def serve_frontend(full_path: str):
        file_path = os.path.join(static_dir, full_path)
        if os.path.isfile(file_path):
            return FileResponse(file_path)
        return FileResponse(os.path.join(static_dir, "index.html"))

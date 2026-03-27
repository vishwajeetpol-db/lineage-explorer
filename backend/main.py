import os
import re
import time
import asyncio
import logging
import threading
from collections import defaultdict

RATE_LIMIT_MAX_REQUESTS = int(os.environ.get("RATE_LIMIT_MAX_REQUESTS", "60"))
RATE_LIMIT_WINDOW_SECONDS = int(os.environ.get("RATE_LIMIT_WINDOW_SECONDS", "60"))
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from databricks.sdk import WorkspaceClient
from backend.lineage_service import (
    list_catalogs,
    list_schemas,
    get_table_lineage,
    get_column_lineage,
    get_columns,
    invalidate_cache,
    _get_client,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Admin group check — cached per user to avoid repeated API calls
# ---------------------------------------------------------------------------
ADMIN_GROUP_NAME = os.environ.get("ADMIN_GROUP_NAME", "admins")
_admin_cache: dict[str, tuple[float, bool]] = {}
_admin_cache_lock = threading.Lock()
ADMIN_CACHE_TTL = 600  # 10 minutes

# Cache token→email resolution to avoid repeated current_user.me() calls
_token_email_cache: dict[str, tuple[float, str]] = {}
_token_email_lock = threading.Lock()
TOKEN_EMAIL_CACHE_TTL = 300  # 5 minutes


def _is_admin(user_email: str) -> bool:
    """Check if a user belongs to the workspace admins group. Results cached 10min."""
    now = time.time()
    with _admin_cache_lock:
        entry = _admin_cache.get(user_email)
        if entry and now - entry[0] < ADMIN_CACHE_TTL:
            return entry[1]

    try:
        w = _get_client()
        # Get current user's groups by listing group membership
        groups = list(w.groups.list(filter=f'displayName eq "{ADMIN_GROUP_NAME}"'))
        if not groups:
            logger.warning(f"Admin group '{ADMIN_GROUP_NAME}' not found in workspace")
            with _admin_cache_lock:
                _admin_cache[user_email] = (now, False)
            return False

        admin_group = groups[0]
        is_member = False
        if admin_group.members:
            # Check if user email matches any member
            member_ids = [m.display for m in admin_group.members if m.display]
            is_member = user_email in member_ids
            if not is_member:
                # Also check by value (which may be user ID)
                # Resolve user email to ID
                users = list(w.users.list(filter=f'userName eq "{user_email}"'))
                if users:
                    user_id = users[0].id
                    is_member = any(m.value == user_id for m in admin_group.members)

        with _admin_cache_lock:
            _admin_cache[user_email] = (now, is_member)
        logger.info(f"Admin check for {user_email}: {is_member}")
        return is_member
    except Exception as e:
        logger.error(f"Error checking admin status for {user_email}: {e}")
        # Fail closed — deny live mode if we can't verify
        with _admin_cache_lock:
            _admin_cache[user_email] = (now, False)
        return False


def _get_user_email(request: Request) -> str | None:
    """Extract user email from Databricks App request.

    Per Databricks Apps docs, the proxy forwards the user's OAuth token
    via the `x-forwarded-access-token` header. We use that token to create
    a user-scoped WorkspaceClient and call current_user.me().

    Ref: https://docs.databricks.com/aws/en/dev-tools/databricks-apps/auth
    """
    user_token = request.headers.get("x-forwarded-access-token")
    if not user_token:
        logger.warning("No x-forwarded-access-token header — cannot identify user")
        return None

    # Check token→email cache first
    token_key = user_token[-16:]
    now = time.time()
    with _token_email_lock:
        cached = _token_email_cache.get(token_key)
        if cached and now - cached[0] < TOKEN_EMAIL_CACHE_TTL:
            return cached[1]

    try:
        host = _get_client().config.host
        user_client = WorkspaceClient(host=host, token=user_token)
        me = user_client.current_user.me()
        if me.user_name:
            with _token_email_lock:
                _token_email_cache[token_key] = (now, me.user_name)
            logger.info(f"Resolved user: {me.user_name}")
            return me.user_name
    except Exception as e:
        logger.error(f"Failed to resolve user from x-forwarded-access-token: {e}")

    return None


# ---------------------------------------------------------------------------
# Input validation — Databricks identifiers must be alphanumeric + underscores
# ---------------------------------------------------------------------------
_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_ .-]{0,254}$")


def _validate_identifier(value: str, name: str) -> str:
    """Validate that a user-supplied identifier is safe for use in SQL."""
    value = value.strip()
    if not value:
        raise HTTPException(status_code=400, detail=f"{name} is required")
    if not _IDENTIFIER_RE.match(value):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid {name}: must be alphanumeric with underscores (got '{value[:50]}')",
        )
    return value


# ---------------------------------------------------------------------------
# Graceful shutdown — clean up threads and caches on SIGTERM
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Lineage Explorer starting up")
    yield
    logger.info("Lineage Explorer shutting down — clearing caches")
    invalidate_cache()


app = FastAPI(title="Lineage Explorer", version="1.2.0", lifespan=lifespan)


# ---------------------------------------------------------------------------
# Rate limiting middleware — protects DBSQL warehouse from abuse
# ---------------------------------------------------------------------------
MAX_TRACKED_IPS = 10_000  # evict oldest IPs beyond this


class RateLimitMiddleware(BaseHTTPMiddleware):
    """In-memory rate limiter: max requests per IP per window, with bounded memory."""

    def __init__(self, app, max_requests: int = 60, window_seconds: int = 60):
        super().__init__(app)
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.requests: dict[str, list[float]] = defaultdict(list)

    async def dispatch(self, request: Request, call_next):
        if not request.url.path.startswith("/api/"):
            return await call_next(request)

        client_ip = request.client.host if request.client else "unknown"
        now = time.time()

        # Evict oldest IPs if we're tracking too many (prevents memory exhaustion)
        if len(self.requests) > MAX_TRACKED_IPS:
            oldest_ips = sorted(
                self.requests.keys(),
                key=lambda ip: self.requests[ip][-1] if self.requests[ip] else 0,
            )
            for ip in oldest_ips[: len(self.requests) - MAX_TRACKED_IPS]:
                del self.requests[ip]

        # Prune old entries for this IP
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


app.add_middleware(RateLimitMiddleware, max_requests=RATE_LIMIT_MAX_REQUESTS, window_seconds=RATE_LIMIT_WINDOW_SECONDS)


# ---------------------------------------------------------------------------
# API endpoints — async wrappers around synchronous SDK calls
# ---------------------------------------------------------------------------

def _safe_error(e: Exception) -> str:
    """Return a sanitized error message safe for API responses (no internal paths/query details)."""
    msg = str(e)
    # Strip internal paths and query text
    if "SQL failed:" in msg:
        return "Query execution failed. Check warehouse availability and permissions."
    if "No SQL warehouse" in msg:
        return "No SQL warehouse available. Configure DATABRICKS_WAREHOUSE_ID."
    if len(msg) > 200:
        return msg[:200] + "..."
    return msg


# ---------------------------------------------------------------------------
# Health check — used by Databricks Apps orchestration
# ---------------------------------------------------------------------------
@app.get("/health")
async def health_check():
    return {"status": "ok", "version": app.version}


# ---------------------------------------------------------------------------
# API endpoints — async wrappers around synchronous SDK calls
# ---------------------------------------------------------------------------

@app.get("/api/user-info")
async def api_user_info(request: Request):
    """Return current user identity and admin status."""
    email = _get_user_email(request)
    if not email:
        return {"email": None, "isAdmin": False}
    is_admin = await asyncio.to_thread(_is_admin, email)
    return {"email": email, "isAdmin": is_admin}


@app.get("/api/catalogs")
async def api_list_catalogs():
    try:
        result = await asyncio.to_thread(list_catalogs)
        return {"catalogs": result}
    except Exception as e:
        logger.error(f"Error listing catalogs: {e}")
        raise HTTPException(status_code=500, detail=_safe_error(e))


@app.get("/api/schemas")
async def api_list_schemas(catalog: str = Query(...)):
    catalog = _validate_identifier(catalog, "catalog")
    try:
        result = await asyncio.to_thread(list_schemas, catalog)
        return {"schemas": result}
    except Exception as e:
        logger.error(f"Error listing schemas: {e}")
        raise HTTPException(status_code=500, detail=_safe_error(e))


@app.get("/api/lineage")
async def api_get_lineage(request: Request, catalog: str = Query(...), schema: str = Query(...), live: bool = Query(False)):
    catalog = _validate_identifier(catalog, "catalog")
    schema = _validate_identifier(schema, "schema")
    # Only admins can bypass cache with live mode
    if live:
        email = _get_user_email(request)
        if not email or not await asyncio.to_thread(_is_admin, email):
            logger.info(f"Live mode denied for {email or 'unknown'} — not in {ADMIN_GROUP_NAME} group")
            live = False
    try:
        if live:
            logger.info(f"LIVE MODE: Serving lineage for {catalog}.{schema} direct from system tables")
        return await asyncio.to_thread(get_table_lineage, catalog, schema, live)
    except Exception as e:
        logger.error(f"Error getting lineage: {e}")
        raise HTTPException(status_code=500, detail=_safe_error(e))


@app.get("/api/columns")
async def api_get_columns(
    request: Request,
    catalog: str = Query(...),
    schema: str = Query(...),
    table: str = Query(...),
    live: bool = Query(False),
):
    """Lazy column loader — fetch columns for a single table on demand."""
    catalog = _validate_identifier(catalog, "catalog")
    schema = _validate_identifier(schema, "schema")
    table = _validate_identifier(table, "table")
    if live:
        email = _get_user_email(request)
        if not email or not await asyncio.to_thread(_is_admin, email):
            live = False
    try:
        cols = await asyncio.to_thread(get_columns, catalog, schema, table, live)
        return {"columns": cols}
    except Exception as e:
        logger.error(f"Error getting columns: {e}")
        raise HTTPException(status_code=500, detail=_safe_error(e))


@app.get("/api/column-lineage")
async def api_get_column_lineage(
    request: Request,
    catalog: str = Query(...),
    schema: str = Query(...),
    table: str = Query(...),
    column: str = Query(...),
    live: bool = Query(False),
):
    catalog = _validate_identifier(catalog, "catalog")
    schema = _validate_identifier(schema, "schema")
    table = _validate_identifier(table, "table")
    column = _validate_identifier(column, "column")
    if live:
        email = _get_user_email(request)
        if not email or not await asyncio.to_thread(_is_admin, email):
            live = False
    try:
        return await asyncio.to_thread(get_column_lineage, catalog, schema, table, column, live)
    except Exception as e:
        logger.error(f"Error getting column lineage: {e}")
        raise HTTPException(status_code=500, detail=_safe_error(e))


@app.post("/api/cache/invalidate")
async def api_invalidate_cache(request: Request):
    """Cache invalidation — protected: only callable from localhost or the app itself."""
    client_ip = request.client.host if request.client else ""
    # In Databricks Apps, requests come through internal proxy (127.0.0.1)
    # Block external direct access
    if client_ip not in ("127.0.0.1", "::1", "localhost"):
        logger.warning(f"Cache invalidation attempt from external IP: {client_ip}")
        raise HTTPException(status_code=403, detail="Cache invalidation is restricted")
    invalidate_cache()
    return {"status": "ok", "message": "Cache cleared"}


# ---------------------------------------------------------------------------
# Serve frontend static files — with path traversal protection
# ---------------------------------------------------------------------------
static_dir = os.path.realpath(os.path.join(os.path.dirname(__file__), "..", "frontend", "dist"))
if os.path.exists(static_dir):
    app.mount("/assets", StaticFiles(directory=os.path.join(static_dir, "assets")), name="assets")

    @app.get("/{full_path:path}")
    async def serve_frontend(full_path: str):
        # Resolve the absolute path and ensure it stays within static_dir
        file_path = os.path.realpath(os.path.join(static_dir, full_path))
        if not file_path.startswith(static_dir):
            # Path traversal attempt
            return FileResponse(os.path.join(static_dir, "index.html"))
        if os.path.isfile(file_path):
            return FileResponse(file_path)
        return FileResponse(os.path.join(static_dir, "index.html"))

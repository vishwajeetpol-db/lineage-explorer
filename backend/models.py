from pydantic import BaseModel
from typing import Optional


class TableNode(BaseModel):
    id: str
    name: str
    full_name: str
    table_type: str
    owner: Optional[str] = None
    comment: Optional[str] = None
    columns: list[dict] = []
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    upstream_count: int = 0
    downstream_count: int = 0


class LineageEdge(BaseModel):
    source: str
    target: str


class ColumnLineageEdge(BaseModel):
    source_table: str
    source_column: str
    target_table: str
    target_column: str


class LineageResponse(BaseModel):
    nodes: list[TableNode]
    edges: list[LineageEdge]
    cached: bool = False
    cached_at: Optional[str] = None


class ColumnLineageResponse(BaseModel):
    edges: list[ColumnLineageEdge]


class CatalogSchema(BaseModel):
    catalogs: list[str] = []
    schemas: list[str] = []

from pydantic import BaseModel
from typing import Literal, Optional, Union


class TableNode(BaseModel):
    node_type: Literal["table"] = "table"
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
    lineage_status: str = "connected"  # connected | root | leaf | orphan


class EntityNode(BaseModel):
    node_type: Literal["entity"] = "entity"
    id: str  # "entity:{type}:{id}"
    entity_type: str  # JOB, NOTEBOOK, PIPELINE, QUERY
    entity_id: str
    display_name: Optional[str] = None
    last_run: Optional[str] = None  # ISO timestamp of latest lineage event
    owner: Optional[str] = None
    cost_usd: Optional[float] = None  # 30-day serverless cost (list price). None = classic compute or no data.


class LineageEdge(BaseModel):
    source: str
    target: str


class ColumnLineageEdge(BaseModel):
    source_table: str
    source_column: str
    target_table: str
    target_column: str


class LineageResponse(BaseModel):
    nodes: list[Union[TableNode, EntityNode]]
    edges: list[LineageEdge]
    cached: bool = False
    cached_at: Optional[str] = None
    cache_expires_at: Optional[str] = None
    fetch_duration_ms: Optional[int] = None


class ColumnLineageResponse(BaseModel):
    edges: list[ColumnLineageEdge]

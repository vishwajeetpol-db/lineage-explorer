"""
Server-side Excel export — builds a styled multi-sheet workbook from a
LineageResponse using openpyxl (a mature OOXML library, so we don't hand-roll
the format). openpyxl is imported lazily inside build_lineage_workbook so the
rest of the app keeps working even if the dependency is somehow missing — the
export endpoint surfaces a clean 503 in that case.

Sheets: Summary, Tables, Lineage (table -> table), Pipelines, Column Lineage.
"""
from __future__ import annotations

import logging
from collections import defaultdict, deque
from datetime import datetime, timezone
from io import BytesIO

logger = logging.getLogger(__name__)


# Palette mirrors the app's dark-UI accent colors (used as cell styling).
_INDIGO = "4F46E5"
_STATUS_FILL = {
    "orphan": "FEF3C7",     # amber
    "root": "E0F2FE",       # sky
    "leaf": "EDE9FE",       # violet
    "connected": "D1FAE5",  # emerald
}
_STATUS_FONT = {
    "orphan": "92400E",
    "root": "075985",
    "leaf": "5B21B6",
    "connected": "065F46",
}


def _is_entity(node_id: str) -> bool:
    return node_id.startswith("entity:")


def _collapse_edges(edges) -> list[tuple[str, str]]:
    """Collapse entity-mediated edges (table -> pipeline -> table) into direct
    table -> table edges; pure table edges pass through."""
    entity_sources: dict[str, set[str]] = {}
    entity_targets: dict[str, set[str]] = {}
    seen: set[tuple[str, str]] = set()
    out: list[tuple[str, str]] = []

    def add(s: str, t: str):
        if s == t or (s, t) in seen:
            return
        seen.add((s, t))
        out.append((s, t))

    for e in edges:
        se, te = _is_entity(e.source), _is_entity(e.target)
        if not se and not te:
            add(e.source, e.target)
        elif not se and te:
            entity_sources.setdefault(e.target, set()).add(e.source)
        elif se and not te:
            entity_targets.setdefault(e.source, set()).add(e.target)

    for entity, sources in entity_sources.items():
        for t in entity_targets.get(entity, ()):
            for s in sources:
                add(s, t)
    return out


def _split_fqdn(full_name: str) -> tuple[str, str]:
    parts = full_name.split(".")
    return (parts[0], parts[1]) if len(parts) == 3 else ("", "")


# Box geometry for the Lineage Map sheet (in cells).
_NODE_W, _NODE_H, _GAP_COL, _GAP_ROW = 4, 3, 1, 2
_TITLE_ROW, _LAYER_HDR_ROW, _FIRST_NODE_ROW = 1, 2, 4


def _layer_nodes(ids, table_edges):
    """Assign each in-scope table a dependency depth (longest path from a source)
    via Kahn's algorithm. Returns (layers_map, orphans, adj_down).

    Pure function (no openpyxl) so the layout logic is unit-testable.
    - layers_map: {layer_index: [full_name, ...]} sorted by table name
    - orphans: tables with no in-scope upstream AND no downstream
    - adj_down: {full_name: set(downstream full_names)} — used for flow arrows
    Cycle-safe: nodes inside a cycle keep their last-assigned layer.
    """
    adj_up: dict[str, set[str]] = {fid: set() for fid in ids}
    adj_down: dict[str, set[str]] = {fid: set() for fid in ids}
    for s, t in table_edges:
        if s in ids and t in ids:
            adj_up[t].add(s)
            adj_down[s].add(t)

    orphans = [fid for fid in ids if not adj_up[fid] and not adj_down[fid]]
    orphan_set = set(orphans)
    connected = [fid for fid in ids if fid not in orphan_set]

    layer: dict[str, int] = {fid: 0 for fid in connected}
    remaining = {fid: sum(1 for u in adj_up[fid] if u in layer) for fid in connected}
    q = deque([fid for fid in connected if remaining[fid] == 0])
    while q:
        n = q.popleft()
        for d in adj_down[n]:
            if d not in layer:
                continue
            layer[d] = max(layer[d], layer[n] + 1)
            remaining[d] -= 1
            if remaining[d] == 0:
                q.append(d)

    layers_map: dict[int, list[str]] = defaultdict(list)
    name_of = lambda f: ids[f].name.lower() if hasattr(ids[f], "name") else f
    for fid in connected:
        layers_map[layer[fid]].append(fid)
    for fids in layers_map.values():
        fids.sort(key=name_of)
    return layers_map, orphans, adj_down


def _build_lineage_map_sheet(wb, table_nodes, table_edges) -> None:
    """Append a 'Lineage Map' sheet: each table is a colored box placed in a
    column by its dependency depth (sources left → downstream right), mirroring
    the app's left-to-right flow. Orphans are grouped at the bottom. Pure cells
    — no drawing objects — so it's safe to open anywhere.
    """
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.comments import Comment
    from openpyxl.utils import get_column_letter

    ids = {t.full_name: t for t in table_nodes}
    if not ids:
        return

    layers_map, orphans, adj_down = _layer_nodes(ids, table_edges)

    ws = wb.create_sheet("Lineage Map")
    ws.sheet_view.showGridLines = False

    thin = Side(style="thin", color="94A3B8")
    box_border = Border(left=thin, right=thin, top=thin, bottom=thin)
    title_font = Font(bold=True, size=12, color=_INDIGO)
    hdr_font = Font(bold=True, size=9, color="64748B")
    arrow_font = Font(bold=True, size=12, color="64748B")

    def draw_box(r0: int, c0: int, node) -> None:
        # Style every cell in the box BEFORE merging: once merged, the inner
        # cells become read-only MergedCell objects that reject style assignment.
        fill = PatternFill("solid", fgColor=_STATUS_FILL.get(node.lineage_status, "E5E7EB"))
        for rr in range(r0, r0 + _NODE_H):
            for cc in range(c0, c0 + _NODE_W):
                cell = ws.cell(row=rr, column=cc)
                cell.fill = fill
                cell.border = box_border
        ws.merge_cells(start_row=r0, start_column=c0, end_row=r0 + _NODE_H - 1, end_column=c0 + _NODE_W - 1)
        tl = ws.cell(row=r0, column=c0)
        tl.value = f"{node.name}\n{node.table_type} · {node.lineage_status}"
        tl.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        tl.font = Font(bold=True, size=10, color=_STATUS_FONT.get(node.lineage_status, "1F2937"))
        note = (
            f"{node.full_name}\n"
            f"upstream: {node.upstream_count}  ·  downstream: {node.downstream_count}"
            + (f"\nowner: {node.owner}" if node.owner else "")
        )
        c = Comment(note, "Lineage Explorer")
        c.width, c.height = 320, 90
        tl.comment = c

    # Title + legend
    title = ws.cell(row=_TITLE_ROW, column=1,
                    value="Lineage map — flows left (sources) → right (downstream).  Hover any box for full name & counts.")
    title.font = title_font

    max_layer = max(layers_map) if layers_map else 0
    for L in range(max_layer + 1):
        col0 = 1 + L * (_NODE_W + _GAP_COL)
        h = ws.cell(row=_LAYER_HDR_ROW, column=col0, value=f"LAYER {L}")
        h.font = hdr_font
        for i, fid in enumerate(layers_map.get(L, [])):
            r0 = _FIRST_NODE_ROW + i * (_NODE_H + _GAP_ROW)
            draw_box(r0, col0, ids[fid])
            if adj_down[fid] and L < max_layer:
                a = ws.cell(row=r0 + _NODE_H // 2, column=col0 + _NODE_W, value="→")
                a.alignment = Alignment(horizontal="center", vertical="center")
                a.font = arrow_font

    # Orphans grouped below the layered area
    if orphans:
        tallest = max((len(v) for v in layers_map.values()), default=0)
        base = _FIRST_NODE_ROW + tallest * (_NODE_H + _GAP_ROW) + 1
        oh = ws.cell(row=base, column=1, value="ORPHANS — no recorded lineage")
        oh.font = Font(bold=True, size=9, color="92400E")
        per_row = max(max_layer + 1, 4)
        orphans.sort(key=lambda f: ids[f].name.lower())
        for k, fid in enumerate(orphans):
            rr = base + 1 + (k // per_row) * (_NODE_H + _GAP_ROW)
            cc = 1 + (k % per_row) * (_NODE_W + _GAP_COL)
            draw_box(rr, cc, ids[fid])

    # Column widths: node columns wide, spacer columns narrow.
    n_orphan_cols = (max(max_layer + 1, 4)) if orphans else 0
    max_cols = max(1 + (max_layer + 1) * (_NODE_W + _GAP_COL), 1 + n_orphan_cols * (_NODE_W + _GAP_COL))
    period = _NODE_W + _GAP_COL
    for col in range(1, max_cols + 1):
        is_spacer = (col - 1) % period == _NODE_W
        ws.column_dimensions[get_column_letter(col)].width = 4 if is_spacer else 9


def build_lineage_workbook(catalog: str, schema: str | None, result, column_edges=None) -> bytes:
    """Build a styled .xlsx (bytes) from a LineageResponse.

    Raises ImportError if openpyxl is unavailable (handled by the caller).
    """
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter

    header_font = Font(bold=True, color="FFFFFF", size=11)
    header_fill = PatternFill("solid", fgColor=_INDIGO)
    header_align = Alignment(horizontal="left", vertical="center")
    header_border = Border(bottom=Side(style="thin", color="CBD5E1"))
    title_font = Font(bold=True, size=15, color=_INDIGO)
    label_font = Font(bold=True, color="334155")
    mono_font = Font(name="Consolas", size=10)

    table_nodes = [n for n in result.nodes if getattr(n, "node_type", None) == "table"]
    entity_nodes = [n for n in result.nodes if getattr(n, "node_type", None) == "entity"]
    table_edges = _collapse_edges(result.edges)
    orphan_count = sum(1 for t in table_nodes if t.lineage_status == "orphan")

    scope = "schema" if schema else "catalog"
    scope_label = f"{catalog}.{schema}" if schema else catalog

    wb = Workbook()

    def style_header(ws, ncols: int):
        for c in range(1, ncols + 1):
            cell = ws.cell(row=1, column=c)
            cell.font = header_font
            cell.fill = header_fill
            cell.alignment = header_align
            cell.border = header_border
        ws.freeze_panes = "A2"
        ws.auto_filter.ref = f"A1:{get_column_letter(ncols)}{ws.max_row}"

    def autosize(ws, headers, rows, mins=None, maxs=None):
        mins = mins or {}
        maxs = maxs or {}
        for i, h in enumerate(headers):
            width = len(str(h))
            for r in rows:
                v = r[i]
                if v is not None:
                    width = max(width, len(str(v)))
            width = min(maxs.get(i, 60), max(mins.get(i, 10), width + 2))
            ws.column_dimensions[get_column_letter(i + 1)].width = width

    # --- Summary ---
    ws = wb.active
    ws.title = "Summary"
    ws["A1"] = "Lineage Explorer — export"
    ws["A1"].font = title_font
    summary = [
        ("Scope", scope),
        ("Target", scope_label),
        ("Generated", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")),
        ("", ""),
        ("Tables", len(table_nodes)),
        ("Pipelines / entities", len(entity_nodes)),
        ("Lineage edges (table → table)", len(table_edges)),
        ("Column lineage edges", len(column_edges.edges) if column_edges else 0),
        ("Tables without lineage (orphan)", orphan_count),
    ]
    for r, (label, value) in enumerate(summary, start=3):
        ws.cell(row=r, column=1, value=label).font = label_font
        if value != "":
            ws.cell(row=r, column=2, value=value)
        if label == "Target":
            ws.cell(row=r, column=2).font = mono_font
    ws.column_dimensions["A"].width = 32
    ws.column_dimensions["B"].width = 48

    # --- Tables ---
    ws = wb.create_sheet("Tables")
    headers = ["Full name", "Name", "Catalog", "Schema", "Type", "Owner",
               "Upstream", "Downstream", "Status", "Columns", "Comment", "Created", "Updated"]
    ws.append(headers)
    rows = []
    for t in sorted(table_nodes, key=lambda n: n.full_name):
        cat, sch = _split_fqdn(t.full_name)
        rows.append([
            t.full_name, t.name, cat, sch, t.table_type, t.owner or "",
            t.upstream_count, t.downstream_count, t.lineage_status,
            len(t.columns or []), t.comment or "", t.created_at or "", t.updated_at or "",
        ])
    status_col = headers.index("Status")  # 0-based
    for r in rows:
        ws.append(r)
        excel_row = ws.max_row
        ws.cell(row=excel_row, column=1).font = mono_font  # Full name mono
        status = r[status_col]
        if status in _STATUS_FILL:
            cell = ws.cell(row=excel_row, column=status_col + 1)
            cell.fill = PatternFill("solid", fgColor=_STATUS_FILL[status])
            cell.font = Font(bold=True, color=_STATUS_FONT[status])
    style_header(ws, len(headers))
    autosize(ws, headers, rows, mins={0: 30, 10: 20}, maxs={0: 60, 5: 36, 10: 60})

    # --- Lineage (table → table) ---
    ws = wb.create_sheet("Lineage")
    ws.append(["Source", "Target"])
    le_rows = [[s, t] for s, t in sorted(table_edges)]
    for r in le_rows:
        ws.append(r)
        ws.cell(row=ws.max_row, column=1).font = mono_font
        ws.cell(row=ws.max_row, column=2).font = mono_font
    style_header(ws, 2)
    autosize(ws, ["Source", "Target"], le_rows, mins={0: 30, 1: 30}, maxs={0: 60, 1: 60})

    # --- Pipelines ---
    if entity_nodes:
        ws = wb.create_sheet("Pipelines")
        headers = ["Type", "Name", "Entity ID", "Last run", "Owner", "Cost (USD, 30d)"]
        ws.append(headers)
        pr_rows = []
        for en in entity_nodes:
            pr_rows.append([
                en.entity_type, en.display_name or "", en.entity_id,
                en.last_run or "", en.owner or "", en.cost_usd if en.cost_usd is not None else "",
            ])
        for r in pr_rows:
            ws.append(r)
            ws.cell(row=ws.max_row, column=3).font = mono_font  # Entity ID
            if isinstance(r[5], (int, float)):
                ws.cell(row=ws.max_row, column=6).number_format = '"$"#,##0.00'
        style_header(ws, len(headers))
        autosize(ws, headers, pr_rows, mins={2: 24}, maxs={1: 50, 2: 44})

    # --- Column Lineage ---
    if column_edges and column_edges.edges:
        ws = wb.create_sheet("Column Lineage")
        headers = ["Source table", "Source column", "Target table", "Target column"]
        ws.append(headers)
        cl_rows = [[e.source_table, e.source_column, e.target_table, e.target_column]
                   for e in column_edges.edges]
        for r in cl_rows:
            ws.append(r)
            ws.cell(row=ws.max_row, column=1).font = mono_font
            ws.cell(row=ws.max_row, column=3).font = mono_font
        style_header(ws, len(headers))
        autosize(ws, headers, cl_rows, mins={0: 28, 2: 28}, maxs={0: 54, 1: 36, 2: 54, 3: 36})

    # --- Lineage Map (last sheet): layered colored boxes, left → right flow ---
    try:
        _build_lineage_map_sheet(wb, table_nodes, table_edges)
    except Exception as map_err:
        # Never let the visual map break the rest of the workbook.
        logger.warning(f"Lineage Map sheet skipped: {map_err}")

    buf = BytesIO()
    wb.save(buf)
    return buf.getvalue()

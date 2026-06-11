"""
Server-side Excel export — builds a styled multi-sheet workbook from a
LineageResponse using openpyxl (a mature OOXML library, so we don't hand-roll
the format). openpyxl is imported lazily inside build_lineage_workbook so the
rest of the app keeps working even if the dependency is somehow missing — the
export endpoint surfaces a clean 503 in that case.

Sheets: Summary, Tables, Lineage (table -> table), Pipelines, Column Lineage.
"""
from __future__ import annotations

from datetime import datetime, timezone
from io import BytesIO


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

    buf = BytesIO()
    wb.save(buf)
    return buf.getvalue()

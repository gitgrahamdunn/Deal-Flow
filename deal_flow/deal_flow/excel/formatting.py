from __future__ import annotations

from openpyxl.formatting.rule import ColorScaleRule
from openpyxl.styles import Font
from openpyxl.worksheet.worksheet import Worksheet


def style_sheet(ws: Worksheet) -> None:
    ws.freeze_panes = "A2"
    for cell in ws[1]:
        cell.font = Font(bold=True)
    ws.auto_filter.ref = ws.dimensions
    for col in ws.columns:
        max_len = max(len(str(c.value)) if c.value is not None else 0 for c in col)
        ws.column_dimensions[col[0].column_letter].width = min(max(max_len + 2, 12), 40)


def add_top_decile_highlight(ws: Worksheet, column_letter: str, rows: int) -> None:
    if rows < 2:
        return
    ws.conditional_formatting.add(
        f"{column_letter}2:{column_letter}{rows}",
        ColorScaleRule(start_type="percentile", start_value=0, start_color="FFF2F2F2", end_type="percentile", end_value=90, end_color="FF1E90FF"),
    )

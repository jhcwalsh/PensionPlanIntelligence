"""Read asset-class returns out of one located window of a document.

The prompt and the schema live here; every model mechanic lives in
``llm_openrouter``. That split is deliberate: MODEL and MAX_OUTPUT_TOKENS are
declared once, in the client. A second copy of the token cap is how
``extract_performance_reports.py`` came to run at 4096 and save thirty
documents with zero rows and no error.
"""
from __future__ import annotations

from decimal import Decimal

from llm_openrouter import call_tool, ResponseTruncated  # re-exported
from section_finder import WINDOW, Candidate

TOOL_NAME = "record_returns"

# The measured trap. On inv-202412.pdf the highest-ranked window is "Total
# Fund Asset Allocation": a grid reading 31.9%, 22.7%, 14.5% — portfolio
# weights, not returns. They are percentages next to asset-class names in a
# document about performance, which is exactly what a return looks like from
# the inside. The instruction has to name the distinction, because the schema
# cannot.
SYSTEM = (
    "You are reading one excerpt from a public pension fund board document. "
    "Record every asset-class RETURN you can see in the excerpt.\n\n"
    "A return is a rate of performance over a period: a 1-year, quarterly, "
    "fiscal-year, 3/5/10-year or since-inception figure.\n\n"
    "Do NOT record an allocation weight as a return. Tables headed 'Asset "
    "Allocation', 'Target Allocation', 'Actual vs Target' or similar list "
    "the share of the portfolio held in each asset class — those percentages "
    "are weights, and they must be ignored entirely. A column headed "
    "'Actual', 'Target', 'Min', 'Max', 'Weight' or '$000s' is not a return.\n\n"
    "Copy every number exactly as printed. Do not compute, convert, annualise "
    "or infer any figure that is not written in the excerpt. If the excerpt "
    "contains no returns at all, record none — an empty list is a correct and "
    "useful answer, and inventing a plausible one is not."
)

RETURNS_SCHEMA = {
    "type": "object",
    "properties": {
        "returns": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "asset_class": {"type": "string"},
                    "return_pct": {"type": "number"},
                    "period": {
                        "type": "string",
                        "description": "as printed, e.g. 'FY2026', 'Q1 2026', '3 Year'",
                    },
                    "benchmark_pct": {"type": "number"},
                },
                "required": ["asset_class", "return_pct", "period"],
            },
        }
    },
    "required": ["returns"],
}


def _call_model(window: str) -> tuple[dict, Decimal]:
    """One seam, so the tests can replace the only paid call."""
    return call_tool(SYSTEM, window, RETURNS_SCHEMA, TOOL_NAME)


def extract_window(text: str, candidate: Candidate) -> tuple[dict, Decimal]:
    """Read the slice ``candidate`` points at.

    Opens 500 characters before the heading so the model sees what introduces
    the table, and stops at WINDOW so the offset stored alongside the result
    names the passage a human can check.
    """
    start = max(0, candidate.offset - 500)
    return _call_model(text[start:start + WINDOW])

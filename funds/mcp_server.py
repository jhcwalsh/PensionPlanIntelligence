"""Stdio MCP server: the fund-crowding tools for an AI IC member.

Run:      python funds/mcp_server.py
Register: claude mcp add fund-crowding -- <repo>/.venv/Scripts/python.exe <repo>/funds/mcp_server.py

Reads Neon through DATABASE_URL (database.py loads .env itself). Needs the
``mcp`` SDK from requirements-mcp.txt, which is kept out of requirements.txt
because Render builds the public web service from that file.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from mcp.server.fastmcp import FastMCP  # noqa: E402

from funds import query  # noqa: E402

server = FastMCP("fund-crowding")

_READING_GUIDE = """
How to read the result:
- holders: other plans whose boards APPROVED a commitment to this fund, with
  amount, % of plan AUM, dates and links to the board documents. Approved only.
- pending: proposed or unclear commitments, and undated mentions of
  unnumbered funds. Not confirmed holdings.
- same_family_other_vintages: holders of other numbered vintages of the same
  fund family. Context, not the same fund.
- manager_only_mentions: commitments to the same manager where no fund was named.
- candidates: the name was ambiguous. Nothing was matched; call again with one
  of these exact names.
- coverage: how many plans and which meeting dates were searched. An empty
  holders list is NOT evidence that no other plan is invested.
Matching is deliberately strict (a false match is worse than a miss), so a
fund spelled very differently by two boards may appear as two funds.
"""


def who_else_is_in(fund: str, exclude_plan: str | None = None) -> dict:
    """Which other U.S. public pension plans have committed to this fund?

    Use when reviewing a proposed commitment, to check crowding: how many
    peer plans are in the same fund, how much, and when. `fund` is a fund
    name such as "GTCR Fund XV" (include the fund number when known).
    `exclude_plan` is the reviewing plan's id, which is left out of the lists
    and anchors the date window for funds without a number.
    """
    return query.who_else_is_in(fund, exclude_plan=exclude_plan)


def funds_held_alongside(fund: str, exclude_plan: str | None = None) -> dict:
    """Which other funds do this fund's holders also commit to?

    Returns funds approved by at least two of the plans that hold `fund`,
    ranked by how many of them, which shows whether a commitment drifts into
    the same cluster of managers as the plan's peers.
    """
    return query.funds_held_alongside(fund, exclude_plan=exclude_plan)


# FastMCP reads the description when the tool is registered, so the guide is
# joined on here rather than appended to __doc__ afterwards.
for _fn in (who_else_is_in, funds_held_alongside):
    server.tool(description=_fn.__doc__ + _READING_GUIDE)(_fn)


if __name__ == "__main__":
    server.run()

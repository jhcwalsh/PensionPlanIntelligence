"""The MCP server exposes both fund tools. Skipped where the SDK isn't
installed: it lives in requirements-mcp.txt, not the CI requirements."""
import asyncio

import pytest

pytest.importorskip("mcp")


def test_server_lists_both_tools():
    from funds.mcp_server import server
    names = {t.name for t in asyncio.run(server.list_tools())}
    assert names == {"who_else_is_in", "funds_held_alongside"}


def test_tool_call_returns_coverage():
    from funds.mcp_server import who_else_is_in
    out = who_else_is_in("Nothing Like This Fund IV")
    assert out["fund"] is None and "coverage" in out


def test_tool_descriptions_carry_reading_guide():
    from funds.mcp_server import server
    for t in asyncio.run(server.list_tools()):
        assert "NOT evidence" in t.description

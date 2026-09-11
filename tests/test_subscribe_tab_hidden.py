"""The Subscribe tab is off the public tab strip (since 2026-09-11).

The sign-up form still exists behind the admin gate, and the magic-link
landing pages keep working for existing subscribers; only the public
entry point is hidden.
"""
import re
import pathlib

SRC = (pathlib.Path(__file__).resolve().parents[1] / "app.py").read_text(encoding="utf-8")


def _tab_block():
    start = SRC.index("tab_specs: list[tuple[str, callable]] = [")
    end = SRC.index("tabs = st.tabs(", start)
    return SRC[start:end]


def test_subscribe_is_not_in_the_public_tab_list():
    block = _tab_block()
    public = block[:block.index("if _admin_unlocked():")]
    assert '"Subscribe"' not in public
    assert '"Plans"' in public


def test_subscribe_is_still_reachable_when_admin_is_unlocked():
    block = _tab_block()
    gated = block[block.index("if _admin_unlocked():"):]
    assert re.search(r'tab_specs\.append\(\("Subscribe"', gated)

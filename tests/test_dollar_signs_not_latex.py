"""Dollar amounts must survive Streamlit's Markdown pass.

These briefings are about money and mention several sums a paragraph.
st.markdown parses $...$ as LaTeX, and `unsafe_allow_html=True` permits HTML
tags without skipping that parse — so two dollar amounts in one sentence
became a maths span. Every weekly briefing on the live site rendered its
headline figures as run-together italics.
"""
import pytest

pytest.importorskip("streamlit")

import app as app_module


REAL = ("TRS-OK (~$19B) led the week's largest single-plan commitment "
        "tranche, approving up to $150 million to TPG Peppertree Fund XI "
        "and up to $150 million to SDC Digital Infrastructure Fund V.")


def test_no_bare_dollar_survives_the_html_conversion():
    """A bare $ anywhere in the output is a LaTeX delimiter waiting to pair."""
    html = app_module._notes_md_to_html(REAL)
    assert "$" not in html
    assert "&#36;19B" in html
    assert "&#36;150 million" in html


def test_the_amounts_are_still_there():
    """Escaping must not eat the figures it protects."""
    html = app_module._notes_md_to_html(REAL)
    assert "19B" in html and "150 million" in html
    assert "TPG Peppertree Fund XI" in html


def test_markdown_structure_still_converts():
    """The escape runs last, over finished HTML — it must not disturb the
    conversion that produced it."""
    html = app_module._notes_md_to_html(
        "## Heading\n\n**Bold** and *italic* and [a link](https://x/y).\n")
    assert "<strong>Bold</strong>" in html
    assert "<em>italic</em>" in html
    assert 'href="https://x/y"' in html


def test_a_dollar_inside_a_link_is_escaped_too():
    """Links carry amounts in their text often enough to matter."""
    html = app_module._notes_md_to_html("[Fund X — $75 million](https://x/y)")
    assert "$" not in html
    assert "&#36;75 million" in html


def test_safe_md_still_guards_the_raw_markdown_surfaces():
    """The Drafts and Archive tabs render markdown directly rather than
    through _notes_md_to_html, and need the backslash form instead."""
    assert app_module._safe_md("up to $150 million") == r"up to \$150 million"


def test_no_content_placeholder_survives_escaping():
    assert app_module._safe_md("_No content._") == "_No content._"

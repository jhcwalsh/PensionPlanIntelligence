"""A keyed widget must survive its options changing underneath it.

Streamlit keeps a keyed widget's selection in session state across reruns and
raises outright when it is later handed a stored value that is not in the
options -- the page dies rather than the selection being quietly dropped.

Nothing in an open browser tab notices that the options changed, and the
quarter list changes for ordinary reasons: a nightly rebuild retires the
oldest quarter, or the staleness cutoff moves. A tab left open across either
of those is enough.
"""
import pytest

pytest.importorskip("streamlit")

import streamlit as st

import app as app_module


KEY = "asset_class_horizon_quarter"


@pytest.fixture(autouse=True)
def _clean_session():
    st.session_state.clear()
    yield
    st.session_state.clear()


def test_a_selection_no_longer_offered_is_removed():
    st.session_state[KEY] = ["2024Q2", "2026Q1"]

    app_module._drop_stale_selection(KEY, ["2026Q1", "2025Q4"])

    assert st.session_state[KEY] == ["2026Q1"]


def test_a_wholly_stale_selection_becomes_empty_rather_than_raising():
    """Empty is the widget's own default, so the page renders unfiltered --
    the behaviour a reader would expect from options that no longer exist."""
    st.session_state[KEY] = ["2014Q3"]

    app_module._drop_stale_selection(KEY, ["2026Q1"])

    assert st.session_state[KEY] == []


def test_a_valid_selection_is_left_exactly_alone():
    """Including its order: rewriting it would reset the widget for no
    reason on every rerun."""
    st.session_state[KEY] = ["2026Q1", "2025Q4"]

    app_module._drop_stale_selection(KEY, ["2026Q3", "2026Q1", "2025Q4"])

    assert st.session_state[KEY] == ["2026Q1", "2025Q4"]


def test_no_stored_selection_is_not_an_error():
    app_module._drop_stale_selection(KEY, ["2026Q1"])
    assert KEY not in st.session_state or st.session_state[KEY] == []


def test_an_empty_option_list_clears_rather_than_raises():
    """Reachable: an empty derived table offers no quarters at all."""
    st.session_state[KEY] = ["2026Q1"]

    app_module._drop_stale_selection(KEY, [])

    assert st.session_state[KEY] == []

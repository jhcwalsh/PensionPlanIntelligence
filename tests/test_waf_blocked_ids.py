"""The WAF-blocked plan IDs must have exactly one source of truth.

Two JSON files already list them, and `pipeline.py` and `refresh_cafrs.py`
each subtract their own list on every run. The Mac Mini job (spec
2026-08-30-mac-mini-migration-design.md, Stage 1) needs the same lists a third
time, to pass them back *in* on the CLI.

A hardcoded copy in a shell script would be correct on the day it was written
and wrong the first time a plan is unblocked -- and wrong silently, because
naming a plan that no longer needs naming still works. These tests bind the
helper to the two loaders that actually gate production.

They also enforce the split the 2026-09-01 probe forced. "Blocked" turned out
to mean three different things, and only one of them is a problem the Mini
solves: 8 of the 11 materials plans are blocked on datacentre IP reputation,
2 return 403 from a residential IP as well, and 1 was never blocked at all --
its discovery selector is stale. Naming the last three on the Mini's CLI would
build a nightly job that fails three plans forever, for reasons no change of
host can fix.
"""

from __future__ import annotations

import json
import pathlib

import pipeline
import refresh_cafrs
from scripts import waf_blocked_ids


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]


# --------------------------------------------------------------------------
# Anti-drift: the helper against the loaders that actually gate production
# --------------------------------------------------------------------------

def test_the_full_materials_list_matches_the_pipeline_loader():
    """If these diverge, the helper is describing a different world than the
    one pipeline.py enforces."""
    assert set(waf_blocked_ids.all_blocked_materials_ids()) == set(
        pipeline._load_waf_blocked_ids())


def test_the_full_cafr_list_matches_the_refresh_cafrs_loader():
    assert set(waf_blocked_ids.all_blocked_cafr_ids()) == set(
        refresh_cafrs._load_waf_blocked_ids())


def test_every_blocked_id_exists_in_the_registry():
    """A typo'd id is accepted by the CLI and silently fetches nothing.

    pipeline.py subtracts unknown ids from the registry without complaint, so
    a misspelling on a block list has no symptom today -- it only appears once
    something tries to fetch that id by name, which is what Stage 1 does.
    """
    with open(REPO_ROOT / "data" / "known_plans.json", encoding="utf-8") as f:
        registry = {p["id"] for p in json.load(f)}
    every = set(waf_blocked_ids.all_blocked_materials_ids()) | set(
        waf_blocked_ids.all_blocked_cafr_ids())
    unknown = sorted(every - registry)
    assert not unknown, f"block lists name ids absent from known_plans.json: {unknown}"


# --------------------------------------------------------------------------
# The residential split -- what the Mini is actually given
# --------------------------------------------------------------------------

def test_the_mini_gets_a_subset_never_a_superset():
    """It can only ever fetch things the cloud is skipping."""
    assert set(waf_blocked_ids.materials_ids()) <= set(
        waf_blocked_ids.all_blocked_materials_ids())
    assert set(waf_blocked_ids.cafr_ids()) <= set(
        waf_blocked_ids.all_blocked_cafr_ids())


def test_the_plans_a_residential_ip_cannot_reach_are_excluded():
    """frs and pgcers_md return 403 from a residential IP too (probed
    2026-09-01). The Mini must not be told to try them."""
    mini = set(waf_blocked_ids.all_ids())
    assert "frs" not in mini
    assert "pgcers_md" not in mini


def test_the_plan_that_was_never_blocked_is_excluded():
    """scers_suffolk answers HTTP 200 with 108 anchors. Its problem is a stale
    selector, which is fixable in the cloud pipeline -- sending the Mini after
    it would hide a scraper bug behind an infrastructure workaround."""
    assert "scers_suffolk" not in set(waf_blocked_ids.all_ids())


def test_unreachable_ids_names_exactly_the_residue():
    """The only plans a paid proxy would still buy. If this set ever empties,
    A4 in nextsteps.md is closed."""
    assert waf_blocked_ids.unreachable_ids() == [
        "frs", "pgcers_md", "scers_suffolk"]


def test_an_unclassified_entry_is_excluded_rather_than_attempted(tmp_path, monkeypatch):
    """The failure mode of forgetting `blocked_by` on a new entry must be that
    the Mini skips it, not that the Mini fails on it every night."""
    path = tmp_path / "blocked.json"
    path.write_text(json.dumps({"plans": [
        {"id": "classified", "blocked_by": "datacentre_ip"},
        {"id": "forgotten"},
        {"id": "typo", "blocked_by": "datacenter_ip"},   # American spelling
    ]}), encoding="utf-8")
    monkeypatch.setattr(waf_blocked_ids, "MATERIALS_FILE", path)

    assert waf_blocked_ids.materials_ids() == ["classified"]
    assert waf_blocked_ids.all_blocked_materials_ids() == [
        "classified", "forgotten", "typo"]


def test_all_ids_is_the_deduplicated_union():
    """asrs and strs_ohio are on both lists; the runner must not fetch twice."""
    expected = set(waf_blocked_ids.materials_ids()) | set(
        waf_blocked_ids.cafr_ids())
    result = waf_blocked_ids.all_ids()
    assert set(result) == expected
    assert len(result) == len(set(result)), "all_ids() contains duplicates"


def test_lists_are_non_empty():
    """Guard on the guards above: an empty list satisfies every set comparison,
    so a helper that silently returned nothing would pass the subset tests and
    build a runner that fetches nothing."""
    assert waf_blocked_ids.materials_ids()
    assert waf_blocked_ids.cafr_ids()
    assert waf_blocked_ids.all_blocked_materials_ids()
    assert waf_blocked_ids.all_blocked_cafr_ids()


# --------------------------------------------------------------------------
# The CLI the runner actually calls
# --------------------------------------------------------------------------

def test_cli_prints_space_separated_materials_ids(capsys):
    rc = waf_blocked_ids.main(["--materials"])
    assert rc == 0
    out = capsys.readouterr().out.strip()
    assert out.split() == waf_blocked_ids.materials_ids()


def test_cli_prints_space_separated_cafr_ids(capsys):
    rc = waf_blocked_ids.main(["--cafr"])
    assert rc == 0
    assert capsys.readouterr().out.strip().split() == waf_blocked_ids.cafr_ids()


def test_cli_defaults_to_the_union(capsys):
    rc = waf_blocked_ids.main([])
    assert rc == 0
    assert capsys.readouterr().out.strip().split() == waf_blocked_ids.all_ids()


def test_cli_all_blocked_is_wider_than_the_default(capsys):
    rc = waf_blocked_ids.main(["--all-blocked"])
    assert rc == 0
    everything = capsys.readouterr().out.strip().split()
    assert set(waf_blocked_ids.all_ids()) < set(everything)
    assert "frs" in everything

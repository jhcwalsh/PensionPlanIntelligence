"""The harness has to be able to report a difference, not just run.

Every function in queries.py returns some mixture of ORM instances, tuples,
dicts and floats. Two backends return equal data in unequal shapes -- Postgres
hands back timezone-aware datetimes and Decimal where SQLite gives naive
datetimes and float -- so normalisation is where this either works or produces
a wall of false positives that hides the one real diff.

See docs/superpowers/plans/2026-08-21-postgres-dual-run.md, Task 2.
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal

import sqlalchemy as sa

import database
from scripts.compare_backends import (
    CASES, compare, first_difference, normalise,
)


def test_first_difference_is_none_when_equal():
    assert first_difference([1, 2, 3], [1, 2, 3]) is None


def test_first_difference_points_deep_into_a_list():
    """The truncated-prefix report was useless: results routinely agree for
    hundreds of characters and diverge at element 40."""
    left = [{"a": i} for i in range(50)]
    right = [{"a": i} for i in range(50)]
    right[40]["a"] = 999
    where, a, b = first_difference(left, right)
    assert where == "[40].a"
    assert (a, b) == (40, 999)


def test_first_difference_reports_a_length_change():
    where, _, _ = first_difference([1, 2], [1, 2, 3])
    assert "length 2 vs 3" in where


def test_first_difference_reports_differing_keys():
    where, a, b = first_difference({"x": 1}, {"y": 1})
    assert "keys differ" in where
    assert (a, b) == (["x"], ["y"])


def test_aware_and_naive_datetimes_compare_equal():
    """SQLite strips the offset on write; Postgres keeps it. Both hold UTC --
    the 2026-08-19 audit established every stored value is UTC either way."""
    naive = dt.datetime(2026, 8, 19, 11, 0, 0)
    aware = dt.datetime(2026, 8, 19, 11, 0, 0, tzinfo=dt.timezone.utc)
    assert normalise(naive) == normalise(aware)


def test_a_real_time_difference_still_shows():
    """Normalising must not flatten everything into equality."""
    a = dt.datetime(2026, 8, 19, 11, 0, 0)
    b = dt.datetime(2026, 8, 19, 12, 0, 0)
    assert normalise(a) != normalise(b)


def test_decimal_and_float_compare_equal():
    """Postgres NUMERIC arrives as Decimal, SQLite REAL as float."""
    assert normalise(Decimal("40.00")) == normalise(40.0)


def test_float_noise_below_the_tolerance_is_ignored():
    assert normalise(7.150000000000001) == normalise(7.15)


def test_a_real_numeric_difference_still_shows():
    assert normalise(7.15) != normalise(7.16)


def test_orm_instances_reduce_to_their_columns():
    """Comparing ORM objects directly compares identity, and comparing repr()
    compares memory addresses -- both mark every row different."""
    doc = database.Document(id=1, plan_id="opers", url="http://x/a.pdf")
    out = normalise(doc)
    assert out["id"] == 1 and out["plan_id"] == "opers"
    assert "_sa_instance_state" not in out


def test_two_equal_orm_instances_normalise_equal():
    """The negative control for the above: distinct objects holding the same
    column values must compare equal, or every row is a false positive."""
    a = database.Document(id=1, plan_id="opers", url="http://x/a.pdf")
    b = database.Document(id=1, plan_id="opers", url="http://x/a.pdf")
    assert a is not b
    assert normalise(a) == normalise(b)


def test_gzipped_text_is_compared_by_digest_not_by_value():
    """extracted_text runs to 2M chars. Holding two corpora of it in memory to
    diff is not an option on the machine that runs the cutover."""
    doc = database.Document(id=1, plan_id="p", extracted_text="x" * 10_000)
    out = normalise(doc)
    assert out["extracted_text"] != "x" * 10_000
    assert len(out["extracted_text"]) == 32          # an md5 hex digest


def test_differing_text_gives_differing_digests():
    a = database.Document(id=1, plan_id="p", extracted_text="alpha")
    b = database.Document(id=1, plan_id="p", extracted_text="beta")
    assert normalise(a)["extracted_text"] != normalise(b)["extracted_text"]


def test_null_text_is_not_hashed():
    doc = database.Document(id=1, plan_id="p", extracted_text=None)
    assert normalise(doc)["extracted_text"] is None


def test_sqlalchemy_rows_are_normalised(tmp_path):
    """sa.Row is a Sequence but NOT a tuple subclass.

    A plain (list, tuple) check misses every multi-column query result -- most
    of queries.py -- and unhandled they fall through to being returned as-is
    and compared by identity, so each such case reports a mismatch that no
    amount of staring at the data explains. This cost a full triage pass on
    the first real run against Neon.
    """
    from sqlalchemy.orm import sessionmaker

    path = _seed(tmp_path / "rows.db")
    engine = sa.create_engine("sqlite:///%s" % path)
    with sessionmaker(bind=engine)() as session:
        row = session.query(database.Plan.id, database.Plan.name).first()
        assert isinstance(row, sa.Row) and not isinstance(row, tuple)
        assert normalise(row) == ["opers", "Ohio PERS"]
    engine.dispose()


def test_datetimes_inside_a_row_are_normalised(tmp_path):
    """The shape that actually bit: an aware datetime nested in a Row.

    Normalising the Row but not recursing would leave Postgres's tz-aware
    value beside SQLite's naive one and report a false mismatch.
    """
    from sqlalchemy.orm import sessionmaker

    path = _seed(tmp_path / "rowdt.db")
    engine = sa.create_engine("sqlite:///%s" % path)
    with sessionmaker(bind=engine)() as session:
        row = session.query(database.CafrRefreshLog.plan_id,
                            database.CafrRefreshLog.run_at).first()
        out = normalise(row)
        assert out[0] == "opers"
        assert isinstance(out[1], str) and out[1].endswith("+00:00"), out


def test_every_public_query_function_has_a_case():
    """The harness is only evidence if it covers the surface. A new query
    function with no case must fail here rather than be silently unchecked.

    "Query" means it reads the database, which in this module means it takes a
    session as its first argument. queries.py also holds pure helpers the
    display layer needs -- period_end and its neighbours parse a period label
    into a quarter -- and there is nothing for two backends to disagree about
    in a function that never touches one. Selecting on the signature rather
    than on a hand-kept exclusion list keeps that distinction honest: give a
    helper a session and it is back in scope automatically.
    """
    import inspect

    import queries

    public = set()
    for name, fn in vars(queries).items():
        if (not inspect.isfunction(fn) or name.startswith("_")
                or fn.__module__ != "queries"):
            continue
        params = list(inspect.signature(fn).parameters)
        if params and params[0] == "session":
            public.add(name)

    assert public - set(CASES) == set(), \
        "queries.py functions with no comparison case: %s" % (public - set(CASES))


def test_no_case_names_a_function_that_does_not_exist():
    """The other direction. A renamed query would otherwise leave a stale case
    that fails at run time, inside the try/except, as a reported 'error' that
    reads like a backend difference."""
    import queries

    missing = {n for n in CASES if not hasattr(queries, n)}
    assert missing == set(), "cases naming absent functions: %s" % missing


def _seed(path, plan_name="Ohio PERS"):
    engine = sa.create_engine("sqlite:///%s" % path)
    database.Base.metadata.create_all(engine)
    with engine.begin() as c:
        c.execute(sa.text("INSERT INTO plans (id, name) VALUES ('opers', :n)"),
                  {"n": plan_name})
        c.execute(sa.text(
            "INSERT INTO cafr_refresh_log (id, plan_id, run_at, status) "
            "VALUES (1, 'opers', '2026-06-01 16:49:57.440368', 'saved')"))
    engine.dispose()
    return str(path)


def test_a_dynamic_case_resolves_to_real_arguments(tmp_path):
    """cafr_refresh_rows filters on run timestamps that only exist in the data.

    With a hardcoded argument it would match nothing on either side, compare
    two empty lists, and pass while testing nothing -- so the resolved
    argument has to be non-empty when rows exist.
    """
    from sqlalchemy.orm import sessionmaker

    from scripts.compare_backends import case_args

    path = _seed(tmp_path / "dyn.db")
    engine = sa.create_engine("sqlite:///%s" % path)
    with sessionmaker(bind=engine)() as session:
        args = case_args(CASES["cafr_refresh_rows"], session)
        assert args[0], "resolved to no run timestamps -- the case is vacuous"
        import queries
        assert queries.cafr_refresh_rows(session, *args), \
            "the resolved timestamps match no rows"
    engine.dispose()


def test_a_literal_case_is_passed_through_unchanged(tmp_path):
    from sqlalchemy.orm import sessionmaker

    from scripts.compare_backends import case_args

    path = _seed(tmp_path / "lit.db")
    engine = sa.create_engine("sqlite:///%s" % path)
    with sessionmaker(bind=engine)() as session:
        assert case_args(CASES["recent_fetch_runs"], session) == (20,)
    engine.dispose()


def test_compare_reports_a_seeded_difference(tmp_path):
    """The test that makes the other tests worth having.

    Two SQLite files standing in for two backends: identical but for one plan
    name. If compare() cannot see that, it cannot see a migration defect
    either, and a clean run against Neon would mean nothing.
    """
    left = _seed(tmp_path / "a.db")
    right = _seed(tmp_path / "b.db", plan_name="Ohio PERS (WRONG)")

    result = compare(left, "sqlite:///%s" % right)
    assert "plans" in result["mismatched"], result


def test_compare_is_clean_on_two_identical_databases(tmp_path):
    """The negative control. Without it, a compare() that reported everything
    as mismatched would still pass the test above."""
    left = _seed(tmp_path / "same_a.db")
    right = _seed(tmp_path / "same_b.db")

    result = compare(left, "sqlite:///%s" % right)
    assert result["mismatched"] == {}, result["mismatched"]
    assert result["errored"] == {}, result["errored"]
    assert len(result["matched"]) == len(CASES)


def test_an_exception_is_reported_not_raised(tmp_path, monkeypatch):
    """One broken query must not abandon the other twenty-four.

    A run that dies partway looks indistinguishable from a clean run that
    covered less than it claimed.
    """
    left = _seed(tmp_path / "e_a.db")
    right = _seed(tmp_path / "e_b.db")

    import queries
    monkeypatch.setattr(queries, "corpus_stats",
                        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")))

    result = compare(left, "sqlite:///%s" % right)
    assert "corpus_stats" in result["errored"]
    assert "boom" in result["errored"]["corpus_stats"]
    assert len(result["matched"]) == len(CASES) - 1

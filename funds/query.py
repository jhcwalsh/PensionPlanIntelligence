"""Answer "who else is in this fund?" from ``funds`` / ``fund_commitments``.

Facts and evidence only — no judgement of whether a fund is crowded; that is
the caller's job. Precision first:

- ``holders`` are approved commitments only. Proposed/unknown go to
  ``pending``; consultant-pipeline and declined rows are never returned.
- An ambiguous query returns ``candidates`` and no holders rather than a guess.
- Other vintages of the same family, and commitments to the manager with no
  fund named, come back in their own sections, never mixed into ``holders``.
- An unnumbered fund only matches mentions within 365 days of an anchor: the
  asking plan's own commitment date for that fund, else today.
- Every answer carries ``coverage`` so an empty list is not read as proof.

Reads only the two small fund tables and ``plans``; never document text.
"""
from __future__ import annotations

import json
from collections import defaultdict
from datetime import date, datetime

from sqlalchemy import func

from database import Fund, FundCommitment, Plan, get_session
from funds.identity import norm_tokens, number_value

UNNUMBERED_WINDOW_DAYS = 365
CAVEAT = ("Built from board commitments in meeting documents we hold. Absence "
          "from this list is not evidence a plan is not invested.")
_SEARCH_STOPWORDS = {"fund", "funds", "the", "lp", "llc", "scsp", "inc", "ltd"}


def _search_tokens(text: str) -> list[str]:
    out = []
    for t in norm_tokens(text):
        if t in _SEARCH_STOPWORDS:
            continue
        n = number_value(t)
        out.append(str(n) if n is not None else t)
    return out


def _query_number(tokens: list[str]) -> int | None:
    nums = [int(t) for t in tokens if t.isdigit()]
    return nums[-1] if nums else None


def _fund_tokens(f: Fund) -> set[str]:
    toks = set(_search_tokens(f"{f.manager_canonical or ''} {f.canonical_name or ''}"))
    if f.number is not None:
        toks.add(str(f.number))
    return toks


def resolve_fund(session, query: str) -> tuple[Fund | None, list[Fund]]:
    q = (query or "").strip()
    # The Crowding page passes the key itself: fetch one row, not every fund.
    if "|" in q:
        hit = session.query(Fund).filter(Fund.fund_key == q).all()
        if len(hit) == 1:
            return hit[0], []
    funds = session.query(Fund).all()
    exact = [f for f in funds if f.fund_key == q
             or (f.canonical_name or "").strip().lower() == q.lower()]
    if len(exact) == 1:
        return exact[0], []
    if len(exact) > 1:
        return None, exact[:10]

    qtoks = _search_tokens(q)
    if not qtoks:
        return None, []
    qnum = _query_number(qtoks)
    cands = []
    for f in funds:
        if qnum is not None and f.number != qnum:
            continue
        if set(qtoks) <= _fund_tokens(f):
            cands.append(f)
    # A candidate whose name is exactly the query wins over co-invest and
    # sidecar vehicles that merely contain it ("FP VIII PA Co-Invest").
    exact_tokens = [c for c in cands if _fund_tokens(c) == set(qtoks)]
    if len(exact_tokens) == 1:
        return exact_tokens[0], []
    # One candidate is only an answer when the query names its whole series:
    # "Ares Pathfinder" must not resolve to Ares Centre Street Pathfinder Core
    # Plus, nor "Blackstone X" to Blackstone Real Estate Partners X, just
    # because those are the only matches we happen to hold.
    if (len(cands) == 1 and (qnum is not None or cands[0].number is None)
            and set(_search_tokens((cands[0].series or "").replace("#", " "))) <= set(qtoks)):
        return cands[0], []
    return None, sorted(cands, key=lambda f: (f.canonical_name or ""))[:10]


def _fund_dict(f: Fund) -> dict:
    return {"name": f.canonical_name, "key": f.fund_key, "manager": f.manager_canonical,
            "series": f.series, "number": f.number, "asset_class": f.asset_class}


def _d(s: str | None) -> date | None:
    return datetime.strptime(s, "%Y-%m-%d").date() if s else None


def _entry(c: FundCommitment, plan: Plan | None) -> dict:
    aum = plan.aum_billions if plan else None
    pct = (round(c.amount_millions / (aum * 1000) * 100, 3)
           if c.amount_millions is not None and aum else None)
    return {"plan_id": c.plan_id, "plan_name": plan.name if plan else None,
            "plan_aum_billions": aum, "amount_millions": c.amount_millions,
            "pct_of_plan_aum": pct, "status": c.status, "first_seen": c.first_seen,
            "last_seen": c.last_seen, "vehicle": c.vehicle,
            "match_confidence": c.match_confidence,
            "evidence": json.loads(c.evidence) if c.evidence else []}


def _plans(session) -> dict[str, Plan]:
    return {p.id: p for p in session.query(Plan).all()}


def _split(session, fund: Fund, exclude_plan, today, plans) -> tuple[list[dict], list[dict]]:
    """Approved holders and pending rows for one fund, after the window rule."""
    rows = session.query(FundCommitment).filter(FundCommitment.fund_id == fund.id).all()
    return _split_rows(rows, fund, exclude_plan, today, plans)


def _split_rows(rows, fund: Fund, exclude_plan, today, plans) -> tuple[list[dict], list[dict]]:
    anchor = today
    if fund.number is None and exclude_plan:
        own = next((r for r in rows if r.plan_id == exclude_plan and r.last_seen), None)
        if own:
            anchor = _d(own.last_seen)

    holders, pending = [], []
    for r in rows:
        if r.plan_id == exclude_plan or r.status in ("pipeline", "declined"):
            continue
        entry = _entry(r, plans.get(r.plan_id))
        if fund.number is None:
            # Each mention must fall in the window on its own; a plan with
            # mentions either side of the anchor does not count via its span.
            dated = [e for e in entry["evidence"] if e.get("meeting_date")]
            if not dated:
                pending.append({**entry, "reason": "undated"})
                continue
            in_window = [e for e in dated
                         if abs((_d(e["meeting_date"]) - anchor).days) <= UNNUMBERED_WINDOW_DAYS]
            if not in_window:
                continue
            approved = any(e.get("status", r.status) == "approved" for e in in_window)
            entry["status"] = "approved" if approved else (
                r.status if r.status != "approved" else "unknown")
        (holders if entry["status"] == "approved" else pending).append(entry)
    holders.sort(key=lambda h: h["last_seen"] or "", reverse=True)
    pending.sort(key=lambda h: h["last_seen"] or "", reverse=True)
    return holders, pending


def _most_crowded(session, today: date, asset_class: str | None = None,
                  min_holders: int = 2, limit: int = 50, all_funds=None) -> list[dict]:
    funds = {f.id: f for f in (all_funds if all_funds is not None else session.query(Fund).all())
             if asset_class is None or f.asset_class == asset_class}
    by_fund: dict[int, list] = defaultdict(list)
    for r in (session.query(FundCommitment)
              .filter(FundCommitment.fund_id.isnot(None),
                      FundCommitment.status.notin_(("pipeline", "declined")))):
        if r.fund_id in funds:
            by_fund[r.fund_id].append(r)
    plans = _plans(session)
    out = []
    for fid, rows in by_fund.items():
        holders, _ = _split_rows(rows, funds[fid], None, today, plans)
        if len(holders) < min_holders:
            continue
        amounts = [h["amount_millions"] for h in holders if h["amount_millions"] is not None]
        out.append({"fund": _fund_dict(funds[fid]), "holders": len(holders),
                    "total_millions": sum(amounts) if amounts else None,
                    "plans": sorted(h["plan_name"] or h["plan_id"] for h in holders),
                    "latest": max((h["last_seen"] or "" for h in holders), default="") or None})
    out.sort(key=lambda r: (-r["holders"], -(r["total_millions"] or 0), r["fund"]["name"] or ""))
    return out[:limit]


def coverage(session) -> dict:
    plans, earliest, latest, as_of = session.query(
        func.count(func.distinct(FundCommitment.plan_id)),
        func.min(FundCommitment.first_seen), func.max(FundCommitment.last_seen),
        func.max(FundCommitment.derived_at)).one()
    return {"plans_searched": plans, "earliest_meeting": earliest, "latest_meeting": latest,
            "as_of": as_of.isoformat() if as_of else None, "caveat": CAVEAT}


def _with_session(fn):
    def wrapper(*args, session=None, **kwargs):
        if session is not None:
            return fn(*args, session=session, **kwargs)
        s = get_session()
        try:
            return fn(*args, session=s, **kwargs)
        finally:
            s.close()
    wrapper.__doc__, wrapper.__name__ = fn.__doc__, fn.__name__
    return wrapper


def _candidates(candidates: list[Fund]) -> list[dict]:
    return [{"name": c.canonical_name, "manager": c.manager_canonical, "number": c.number}
            for c in candidates]


def _who_else(session, f: Fund, exclude_plan, today, plans) -> dict:
    holders, pending = _split(session, f, exclude_plan, today, plans)
    out = {"fund": _fund_dict(f), "holders": holders, "pending": pending,
           "same_family_other_vintages": [], "manager_only_mentions": []}

    if f.number is not None:
        family = (session.query(Fund)
                  .filter(Fund.manager_canonical == f.manager_canonical, Fund.series == f.series,
                          Fund.number.isnot(None), Fund.number != f.number)
                  .order_by(Fund.number.desc()).all())
        # One query for every vintage's rows, not one per vintage.
        rows_by_fund: dict[int, list] = defaultdict(list)
        if family:
            for r in (session.query(FundCommitment)
                      .filter(FundCommitment.fund_id.in_([v.id for v in family]))):
                rows_by_fund[r.fund_id].append(r)
        for v in family:
            v_holders, _ = _split_rows(rows_by_fund[v.id], v, exclude_plan, today, plans)
            if v_holders:
                out["same_family_other_vintages"].append({"fund": _fund_dict(v), "holders": v_holders})

    mgr_rows = (session.query(FundCommitment)
                .filter(FundCommitment.fund_id.is_(None),
                        FundCommitment.manager_canonical == f.manager_canonical,
                        FundCommitment.status.notin_(("pipeline", "declined")))
                .all())
    out["manager_only_mentions"] = [_entry(r, plans.get(r.plan_id)) for r in mgr_rows
                                    if r.plan_id != exclude_plan]
    return out


def _alongside(session, f: Fund, holders: list[dict], plans, limit: int) -> dict:
    holder_ids = {h["plan_id"] for h in holders}
    out = {"fund": _fund_dict(f), "holders_considered": len(holder_ids), "alongside": []}
    if not holder_ids:
        return out

    by_fund: dict[int, set[str]] = defaultdict(set)
    for fund_id, plan_id in (session.query(FundCommitment.fund_id, FundCommitment.plan_id)
                             .filter(FundCommitment.plan_id.in_(holder_ids),
                                     FundCommitment.status == "approved",
                                     FundCommitment.fund_id.isnot(None),
                                     FundCommitment.fund_id != f.id)):
        by_fund[fund_id].add(plan_id)
    keep = {fid: ps for fid, ps in by_fund.items() if len(ps) >= 2}
    funds = {x.id: x for x in session.query(Fund).filter(Fund.id.in_(keep))} if keep else {}
    rows = [{"fund": _fund_dict(funds[fid]),
             "plans": [{"plan_id": p, "plan_name": plans[p].name if p in plans else None}
                       for p in sorted(ps)],
             "count": len(ps), "share_of_holders": round(len(ps) / len(holder_ids), 3)}
            for fid, ps in keep.items()]
    rows.sort(key=lambda r: (-r["count"], r["fund"]["name"] or ""))
    out["alongside"] = rows[:limit]
    return out


@_with_session
def who_else_is_in(fund: str, exclude_plan: str | None = None, session=None,
                   today: date | None = None) -> dict:
    """Other plans committed to ``fund``, with evidence and coverage."""
    today = today or date.today()
    f, candidates = resolve_fund(session, fund)
    result = {"query": fund, "fund": None, "candidates": _candidates(candidates),
              "holders": [], "pending": [], "same_family_other_vintages": [],
              "manager_only_mentions": [], "coverage": coverage(session)}
    if f is not None:
        result.update(_who_else(session, f, exclude_plan, today, _plans(session)))
    return result


@_with_session
def funds_held_alongside(fund: str, exclude_plan: str | None = None, session=None,
                         today: date | None = None, limit: int = 25) -> dict:
    """Funds that the holders of ``fund`` also committed to, held by 2+ of them."""
    today = today or date.today()
    f, candidates = resolve_fund(session, fund)
    result = {"query": fund, "fund": None, "candidates": _candidates(candidates),
              "holders_considered": 0, "alongside": [], "coverage": coverage(session)}
    if f is not None:
        plans = _plans(session)
        holders, _ = _split(session, f, exclude_plan, today, plans)
        result.update(_alongside(session, f, holders, plans, limit))
    return result


@_with_session
def fund_detail(fund: str, exclude_plan: str | None = None, session=None,
                today: date | None = None, alongside_limit: int = 25) -> dict:
    """``who_else_is_in`` and ``funds_held_alongside`` in one pass, for the
    Crowding page: the fund, plans, coverage and holders are read once."""
    today = today or date.today()
    f, candidates = resolve_fund(session, fund)
    cov = coverage(session)
    who = {"query": fund, "fund": None, "candidates": _candidates(candidates),
           "holders": [], "pending": [], "same_family_other_vintages": [],
           "manager_only_mentions": [], "coverage": cov}
    along = {"query": fund, "fund": None, "candidates": _candidates(candidates),
             "holders_considered": 0, "alongside": [], "coverage": cov}
    if f is not None:
        plans = _plans(session)
        who.update(_who_else(session, f, exclude_plan, today, plans))
        along.update(_alongside(session, f, who["holders"], plans, alongside_limit))
    return {"who": who, "alongside": along}


@_with_session
def most_crowded(asset_class: str | None = None, min_holders: int = 2, limit: int = 50,
                 session=None, today: date | None = None) -> list[dict]:
    """Funds ranked by how many plans hold them (approved commitments, after
    the unnumbered-fund window), with summed stated amounts."""
    return _most_crowded(session, today or date.today(), asset_class, min_holders, limit)


@_with_session
def overview(session=None, today: date | None = None) -> dict:
    """Headline counts for the Crowding page, every fund as {key: name}, and
    the full ranked crowded list, so the page filters it by asset class
    without going back to the database."""
    funds = session.query(Fund).all()
    names = sorted(((f.fund_key, f.canonical_name) for f in funds if f.canonical_name),
                   key=lambda kv: (kv[1].lower(), kv[0]))
    crowded = _most_crowded(session, today or date.today(), limit=10**9, all_funds=funds)
    return {"funds": len(funds), "crowded": len(crowded), "crowded_funds": crowded,
            "fund_index": dict(names), "coverage": coverage(session)}

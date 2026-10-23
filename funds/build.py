"""Build ``funds`` / ``fund_commitments`` from board investment actions.

Two stages:

1. ``extract_pending`` sends every commitment/hire action not yet in
   ``fund_action_extracts`` to Haiku (funds/extract.py), cached by content
   hash, so a nightly run only pays for new actions.
2. ``rebuild`` re-derives both tables from the cache and the summaries, in one
   transaction. It is deterministic and cheap, so edits to
   ``data/fund_mappings.json`` or ``data/manager_mappings.json`` take effect on
   the next rebuild with no model calls.

Identity is exact-key only (funds/identity.py) plus the human-reviewed alias
file. Nothing here merges on similarity: a false match is worse than a miss.
"""
from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path

from database import Document, Fund, FundActionExtract, FundCommitment, Meeting, Summary
from funds.extract import BATCH_SIZE, MODEL, ActionInput, action_hash, extract_batch
from funds.identity import fund_key, manager_tokens, norm_manager, norm_tokens, parse_fund_name
from twin_builder import (
    MANAGER_MAPPINGS_PATH,
    _parse_json_list,
    canonical_asset_class,
    load_asset_class_mappings,
)

FUND_MAPPINGS_PATH = Path(__file__).resolve().parent.parent / "data" / "fund_mappings.json"
ACTIONS = ("commitment", "hire")
QUOTE_CHARS = 300


def load_manager_mappings() -> dict[str, str | None]:
    """Raw manager string -> canonical display name, or None when the
    normaliser judged it not a manager ("Various", a plan's own name)."""
    try:
        raw = json.loads(MANAGER_MAPPINGS_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    out = {}
    for name, m in raw.items():
        if isinstance(m, dict):
            out[name] = (m.get("canonical") or name) if m.get("is_manager", True) else None
        else:
            out[name] = m or name
    return out


def load_fund_mappings() -> dict:
    try:
        data = json.loads(FUND_MAPPINGS_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        data = {}
    data.setdefault("aliases", {})
    data.setdefault("separate", [])
    return data


def resolve_alias(key: str, aliases: dict) -> tuple[str, bool]:
    used = False
    for _ in range(5):
        if key not in aliases:
            break
        key, used = aliases[key], True
    return key, used


def _iso(dt) -> str | None:
    return dt.strftime("%Y-%m-%d") if dt else None


def collect_actions(session) -> list[tuple[ActionInput, dict]]:
    rows = (
        session.query(Summary.document_id, Summary.investment_actions,
                      Summary.model_used, Summary.text_hash,
                      Document.plan_id, Document.doc_type, Document.url,
                      Document.meeting_date, Meeting.meeting_date)
        .join(Document, Document.id == Summary.document_id)
        .outerjoin(Meeting, Meeting.id == Document.meeting_id)
        .filter(Summary.investment_actions.isnot(None))
        .all()
    )
    # A dedup summary copies the actions of whichever document first had the
    # same text. Credit it only to the plan that document belongs to: a shared
    # consultant deck must not make plan Q a holder of plan P's commitment.
    original_plan = {r.text_hash: r.plan_id for r in rows
                     if r.text_hash and not (r.model_used or "").startswith("dedup:")}
    out = []
    for doc_id, actions_json, model_used, text_hash, plan_id, doc_type, url, doc_date, mtg_date in rows:
        if (model_used or "").startswith("dedup:") and original_plan.get(text_hash) != plan_id:
            continue
        date = _iso(doc_date or mtg_date)
        for action in _parse_json_list(actions_json):
            if not isinstance(action, dict) or action.get("action") not in ACTIONS:
                continue
            inp = ActionInput(action_hash(action), action, doc_type, date)
            out.append((inp, {"document_id": doc_id, "plan_id": plan_id,
                              "meeting_date": date, "doc_type": doc_type, "url": url}))
    return out


def extract_pending(session, client, limit: int | None = None) -> tuple[int, int]:
    done = {h for (h,) in session.query(FundActionExtract.action_hash)}
    pending: dict[str, ActionInput] = {}
    for inp, _ in collect_actions(session):
        if inp.action_hash not in done:
            pending.setdefault(inp.action_hash, inp)
    todo = list(pending.values())[:limit] if limit else list(pending.values())

    extracted = failed = 0
    # A batch that fails (typically a response cut off by max_tokens on a
    # bundle-heavy batch) is halved and retried, so one dense batch cannot
    # fail the same way every night.
    queue = [todo[i:i + BATCH_SIZE] for i in range(0, len(todo), BATCH_SIZE)]
    while queue:
        batch = queue.pop(0)
        try:
            result = extract_batch(client, batch)
        except Exception as exc:  # noqa: BLE001 - one bad batch must not stop the run
            session.rollback()
            if len(batch) > 1:
                half = len(batch) // 2
                queue[:0] = [batch[:half], batch[half:]]
                continue
            print(f"  action {batch[0].action_hash[:10]} FAILED: {type(exc).__name__}: {exc}")
            failed += 1
            continue
        for h, items in result.items():
            session.add(FundActionExtract(action_hash=h, items_json=json.dumps(items),
                                          model="mock" if client is None else MODEL))
        session.commit()
        extracted += len(result)
    return extracted, failed


def _canonical_manager(raw: str | None, mappings: dict) -> str | None:
    if not raw or not raw.strip():
        return None
    raw = raw.strip()
    return mappings[raw] if raw in mappings else raw


def _parse(fund_name: str, raw_manager: str | None, display: str | None):
    """Parse against the canonical manager, and also against the raw string
    when it is another spelling of the manager, so 'KKR North America XIV' and
    'Kohlberg Kravis Roberts North America Fund XIV' (both canonical KKR) land
    on the same key.

    A raw string that *extends* the canonical name is not a spelling of the
    manager but a fund family ('Carlyle Realty Partners' under The Carlyle
    Group); stripping it would wipe the series and merge Carlyle Realty
    Partners VIII with Carlyle Partners VIII. Those are never stripped."""
    managers = [display] if display else []
    if raw_manager and raw_manager != display:
        canon = manager_tokens(display)
        raw = [t for t in norm_tokens(raw_manager) if t not in ("the",)]
        if not canon or raw[:len(canon)] != canon:
            managers.append(raw_manager)
    options = [parse_fund_name(fund_name, m) for m in managers]
    options = options or [parse_fund_name(fund_name, None)]
    return min(options, key=lambda p: (len(p.series.split()), p.series))


def _num(v):
    return v if isinstance(v, (int, float)) and not isinstance(v, bool) else None


def _most_common(values):
    c = Counter(v for v in values if v)
    if not c:
        return None
    return max(c.items(), key=lambda kv: (kv[1], len(kv[0]), kv[0]))[0]


def rebuild(session) -> dict:
    manager_map = load_manager_mappings()
    asset_map = load_asset_class_mappings()
    aliases = load_fund_mappings()["aliases"]
    cache = {h: json.loads(items)
             for h, items in session.query(FundActionExtract.action_hash,
                                           FundActionExtract.items_json)}

    groups: dict[tuple, list[dict]] = defaultdict(list)
    displays: dict[str, Counter] = defaultdict(Counter)
    dropped = 0

    for inp, ctx in collect_actions(session):
        items = cache.get(inp.action_hash)
        if items is None:
            continue
        act = inp.action
        for item in items:
            fund_name = item.get("fund_name")
            if act.get("action") == "hire" and not fund_name:
                dropped += 1
                continue
            # Haiku sees the action's amount and decides which fund it belongs
            # to; falling back to the action total would put a bundle's whole
            # total on the one fund it managed to name.
            amount = _num(item.get("amount_millions"))
            raw_mgr = item.get("manager") or act.get("manager")
            display = _canonical_manager(raw_mgr, manager_map)
            mnorm = norm_manager(display)
            ac = canonical_asset_class(item.get("asset_class") or act.get("asset_class"), asset_map)
            obs = {**ctx, "status": item.get("status") or "unknown", "amount": amount,
                   "fund_name_raw": fund_name, "asset_class": None if ac == "unmapped" else ac,
                   "quote": (act.get("description") or "")[:QUOTE_CHARS],
                   "vehicle": None, "aliased": False}
            if mnorm:
                displays[mnorm][display] += 1

            if not fund_name:
                if not mnorm:
                    dropped += 1
                    continue
                groups[(ctx["plan_id"], "manager", mnorm)].append(obs)
                continue

            parsed = _parse(fund_name, raw_mgr, display)
            key = fund_key(mnorm, parsed)
            if key is None:
                # A fund name too generic to identify ("Fund III") still says
                # the plan committed to this manager.
                if not mnorm:
                    dropped += 1
                    continue
                groups[(ctx["plan_id"], "manager", mnorm)].append(obs)
                continue
            key, obs["aliased"] = resolve_alias(key, aliases)
            obs["vehicle"] = parsed.vehicle
            groups[(ctx["plan_id"], "fund", key)].append(obs)

    def display_for(mnorm: str) -> str:
        return _most_common(displays[mnorm]) or mnorm

    fund_obs: dict[str, list[dict]] = defaultdict(list)
    for (plan_id, kind, key), obs in groups.items():
        if kind == "fund":
            fund_obs[key].extend(obs)

    session.query(FundCommitment).delete()
    session.query(Fund).delete()
    session.flush()

    funds_by_key = {}
    for key, obs in fund_obs.items():
        mnorm, series, number = key.split("|")
        f = Fund(fund_key=key,
                 canonical_name=_most_common(o["fund_name_raw"] for o in obs),
                 manager_canonical=display_for(mnorm), series=series,
                 number=int(number) if number else None,
                 asset_class=_most_common(o["asset_class"] for o in obs))
        session.add(f)
        funds_by_key[key] = f
    session.flush()

    commitments = manager_only = 0
    for (plan_id, kind, key), obs in groups.items():
        obs.sort(key=lambda o: o["meeting_date"] or "")
        approved = [o for o in obs if o["status"] == "approved"]
        status = "approved" if approved else obs[-1]["status"]
        amounts = [o["amount"] for o in (approved or obs) if o["amount"] is not None]
        dates = [o["meeting_date"] for o in obs if o["meeting_date"]]
        by_doc: dict[int, dict] = {}
        for o in obs:
            e = by_doc.get(o["document_id"])
            if e is None:
                by_doc[o["document_id"]] = {k: o[k] for k in
                                            ("document_id", "meeting_date", "doc_type", "url", "quote", "status")}
            elif o["status"] == "approved":
                e["status"] = "approved"
        evidence = list(by_doc.values())

        if kind == "fund":
            fund = funds_by_key[key]
            confidence = ("alias" if any(o["aliased"] for o in obs)
                          else "unnumbered" if fund.number is None else "exact")
            mgr = fund.manager_canonical
            commitments += 1
        else:
            fund, confidence, mgr = None, "manager_only", display_for(key)
            manager_only += 1

        session.add(FundCommitment(
            plan_id=plan_id, fund_id=fund.id if fund else None, manager_canonical=mgr,
            fund_name_raw=_most_common(o["fund_name_raw"] for o in obs),
            vehicle=_most_common(o["vehicle"] for o in obs),
            asset_class=_most_common(o["asset_class"] for o in obs),
            amount_millions=amounts[-1] if amounts else None, status=status,
            first_seen=min(dates) if dates else None, last_seen=max(dates) if dates else None,
            evidence=json.dumps(evidence), match_confidence=confidence,
        ))
    session.commit()
    return {"funds": len(funds_by_key), "commitments": commitments,
            "manager_only": manager_only, "dropped": dropped}

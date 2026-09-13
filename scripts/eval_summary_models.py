"""Side-by-side: the production summary prompt on Sonnet 5 and an open-weight
model through OpenRouter, on the documents the summariser routes to Sonnet.

Why this exists. Sonnet 5 is about $8 of a $12 monthly bill, and the
investment-pack summaries are the largest line inside that. DeepSeek V4
Flash costs a twentieth as much and is already wired for section reads.
Whether it can take the summaries is a question about quality, and there
was no eval to answer it. This script produces the material for one: the
same prompt, the same truncated text, both models, every output kept.

    python scripts/eval_summary_models.py --n 20 --out eval_out

Writes <out>/summary_models_<date>.json (every request and response, with
tokens, cost and latency) and <out>/summary_models_<date>.md (a report to
read: per document, each model's summary and its counts of decisions,
actions, performance rows and notable items, with the stored production
summary alongside where one exists).

Spend: about 6 cents per document on Sonnet 5 at the synchronous rate, a
fraction of a cent on DeepSeek. Both sides land in api_usage under the
operation "eval:summary_models".
"""
from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

from sqlalchemy.orm import undefer

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

import costs  # noqa: E402
import database
import llm_openrouter
import summarizer
from database import Document, Plan, Summary

OPERATION = "eval:summary_models"
OPEN_MODEL = llm_openrouter.MODEL          # overridden by --open-model
REASONING_OFF = False                      # set by --reasoning off


def pick_documents(session, n: int, per_plan: int, days: int) -> list[Document]:
    """The most recent extracted documents the summariser would send to
    Sonnet, at most ``per_plan`` from any one plan so the sample is not one
    fund's house style twenty times over.

    Candidates are pre-filtered on file size before the text is loaded:
    choose_model needs the text, but loading it for every recent document
    would pull megabytes over the wire to discard most of them.
    """
    since = datetime.now(timezone.utc) - timedelta(days=days)
    q = (session.query(Document)
         .options(undefer(Document.extracted_text))
         .filter(Document.extraction_status == "done",
                 Document.downloaded_at >= since,
                 Document.file_size_bytes >= 100_000)
         .order_by(Document.downloaded_at.desc()))
    chosen: list[Document] = []
    counts: dict[str, int] = {}
    seen_text: set[str] = set()
    for doc in q.limit(400):
        if summarizer.choose_model(doc) != summarizer.MODEL_SONNET:
            continue
        if summarizer.should_skip(doc) or not doc.meeting_date:
            continue
        # The same text under two URLs is one test, not two.
        digest = hashlib.md5((doc.extracted_text or "").encode()).hexdigest()
        if digest in seen_text:
            continue
        seen_text.add(digest)
        if counts.get(doc.plan_id, 0) >= per_plan:
            continue
        counts[doc.plan_id] = counts.get(doc.plan_id, 0) + 1
        chosen.append(doc)
        if len(chosen) >= n:
            break
    return chosen


def call_sonnet(prompt: str) -> dict:
    t0 = time.monotonic()
    message = summarizer._get_client().messages.create(
        **summarizer.request_params(prompt, summarizer.MODEL_SONNET))
    latency = time.monotonic() - t0
    raw = summarizer.message_text(message)
    return {
        "model": summarizer.MODEL_SONNET,
        "raw": raw,
        "parsed": _parse(raw),
        "input_tokens": message.usage.input_tokens,
        "output_tokens": message.usage.output_tokens,
        "cost_usd": float(costs.cost_usd(summarizer.MODEL_SONNET, message.usage)),
        "latency_s": round(latency, 1),
        "stop_reason": message.stop_reason,
    }


def call_open(prompt: str) -> dict:
    """The same system prompt and user prompt, JSON mode instead of a tool
    schema: the prompt already asks for a bare JSON object, and the point
    is to test the model on the production prompt, not a rewritten one."""
    t0 = time.monotonic()
    resp = llm_openrouter._raw_call(
        model=OPEN_MODEL,
        max_tokens=6000,
        messages=[{"role": "system", "content": summarizer.SYSTEM_PROMPT},
                  {"role": "user", "content": prompt}],
        response_format={"type": "json_object"},
        # Reasoning off matches production, where Sonnet runs with thinking
        # disabled. Left on, DeepSeek V4 Flash spent the whole 6,000-token
        # budget thinking on two of twenty packs and returned nothing.
        extra_body={"provider": {"require_parameters": True},
                    **({"reasoning": {"enabled": False}} if REASONING_OFF else {})},
    )
    latency = time.monotonic() - t0
    usage = llm_openrouter.adapt_usage(resp.usage)
    if not costs.mock_mode():
        database.record_api_usage(OPEN_MODEL, usage)
    choice = resp.choices[0]
    raw = choice.message.content or ""
    return {
        "model": OPEN_MODEL,
        "raw": raw,
        "parsed": _parse(raw),
        "input_tokens": usage.input_tokens,
        "output_tokens": usage.output_tokens,
        "cost_usd": float(costs.cost_usd(OPEN_MODEL, usage)),
        "latency_s": round(latency, 1),
        "stop_reason": choice.finish_reason,
    }


def _parse(raw: str):
    try:
        return summarizer.parse_response(raw)
    except ValueError as exc:
        return {"_parse_error": str(exc)}


def _stored(session, doc: Document) -> dict | None:
    s = session.query(Summary).filter_by(document_id=doc.id).first()
    if not s:
        return None
    return {
        "model": s.model_used,
        "parsed": {
            "summary": s.summary_text,
            "key_topics": _loads(s.key_topics),
            "decisions": _loads(s.decisions),
            "investment_actions": _loads(s.investment_actions),
            "performance_data": _loads(s.performance_data),
        },
    }


def _loads(text):
    try:
        return json.loads(text) if text else []
    except ValueError:
        return []


def _counts(parsed: dict) -> str:
    if not isinstance(parsed, dict) or "_parse_error" in parsed:
        return "PARSE ERROR"
    n = lambda k: len(parsed.get(k) or []) if isinstance(parsed.get(k), list) else "?"
    return (f"decisions {n('decisions')}, actions {n('investment_actions')}, "
            f"performance {n('performance_data')}, notable {n('notable_items')}, "
            f"topics {n('key_topics')}")


def _block(title: str, r: dict | None) -> list[str]:
    if r is None:
        return [f"**{title}**: none", ""]
    p = r["parsed"]
    lines = [f"**{title}** ({r.get('model')})"]
    if "cost_usd" in r:
        lines.append(f"cost ${r['cost_usd']:.4f}, {r['input_tokens']}->{r['output_tokens']} tokens, "
                     f"{r['latency_s']}s, stop={r['stop_reason']}")
    lines.append(_counts(p))
    if isinstance(p, dict) and "_parse_error" not in p:
        lines += ["", "> " + str(p.get("summary", "")).replace("\n", " "), ""]
        for key in ("decisions", "investment_actions", "performance_data", "notable_items"):
            items = p.get(key) or []
            if items:
                lines.append(f"- {key}:")
                for it in items[:12]:
                    lines.append(f"  - {json.dumps(it, ensure_ascii=False)[:300]}")
                if len(items) > 12:
                    lines.append(f"  - ... {len(items) - 12} more")
    else:
        lines += ["", "```", r.get("raw", "")[:1500], "```"]
    lines.append("")
    return lines


def write_report(records: list[dict], path: pathlib.Path) -> None:
    out = [f"# Summary model comparison, {datetime.now():%Y-%m-%d}", ""]
    tot = {}
    for r in records:
        for key in ("sonnet", "open"):
            x = r[key]
            t = tot.setdefault(key, {"cost": 0.0, "lat": 0.0, "parse_err": 0, "n": 0})
            t["cost"] += x["cost_usd"]; t["lat"] += x["latency_s"]; t["n"] += 1
            t["parse_err"] += int("_parse_error" in x["parsed"])
    out.append("| Model | Docs | Total cost | Mean latency | Parse errors |")
    out.append("|---|---|---|---|---|")
    for key, label in (("sonnet", summarizer.MODEL_SONNET), ("open", OPEN_MODEL)):
        t = tot[key]
        out.append(f"| {label} | {t['n']} | ${t['cost']:.3f} | {t['lat']/max(t['n'],1):.1f}s | {t['parse_err']} |")
    out.append("")
    for i, r in enumerate(records, 1):
        out += [f"## {i}. {r['plan']} — {r['filename']}",
                f"{r['doc_type']}, {r['meeting_date']}, {r['chars']:,} chars sent (doc {r['doc_id']})", ""]
        out += _block("Sonnet 5", r["sonnet"])
        out += _block("Open model", r["open"])
        out += _block("Stored production summary", r["stored"])
    path.write_text("\n".join(out), encoding="utf-8")


def main() -> None:
    global OPEN_MODEL, REASONING_OFF
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=20)
    ap.add_argument("--per-plan", type=int, default=2)
    ap.add_argument("--days", type=int, default=120)
    ap.add_argument("--out", default="eval_out")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--open-model", default=llm_openrouter.MODEL,
                    help="OpenRouter model id; needs a row in costs.PRICES")
    ap.add_argument("--reasoning", choices=["default", "off"], default="default")
    ap.add_argument("--reuse-sonnet", metavar="JSON",
                    help="a previous run's JSON: reuse its Sonnet outputs for the "
                         "same documents instead of paying for them again")
    args = ap.parse_args()
    OPEN_MODEL = args.open_model
    REASONING_OFF = args.reasoning == "off"
    reuse = {}
    if args.reuse_sonnet:
        for r in json.loads(pathlib.Path(args.reuse_sonnet).read_text(encoding="utf-8")):
            reuse[r["doc_id"]] = r["sonnet"]

    if not llm_openrouter.have_key():
        raise SystemExit("OPENROUTER_API_KEY not set")

    out_dir = pathlib.Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d") + "_" + args.open_model.split("/")[-1]         + ("_noreason" if REASONING_OFF else "")

    session = database.get_session()
    try:
        docs = pick_documents(session, args.n, args.per_plan, args.days)
        plans = {p.id: p.name for p in session.query(Plan).all()}
        jobs = []
        for doc in docs:
            truncated = summarizer.smart_truncate(doc.extracted_text)
            jobs.append({
                "doc_id": doc.id, "plan": plans.get(doc.plan_id, doc.plan_id),
                "plan_id": doc.plan_id, "filename": doc.filename,
                "doc_type": doc.doc_type,
                "meeting_date": doc.meeting_date.date().isoformat() if doc.meeting_date else None,
                "chars": len(truncated),
                "prompt": summarizer.build_extraction_prompt(doc, plans.get(doc.plan_id, doc.plan_id), truncated),
                "stored": _stored(session, doc),
            })
        session.commit()
    finally:
        session.close()

    print(f"{len(jobs)} documents; running both models with {args.workers} workers")

    def run(job):
        with costs.track(OPERATION, run_id=str(job["doc_id"])):
            job["sonnet"] = reuse.get(job["doc_id"]) or call_sonnet(job["prompt"])
            job["open"] = call_open(job["prompt"])
        print(f"  done {job['plan']} / {job['filename']}: "
              f"sonnet ${job['sonnet']['cost_usd']:.4f} {job['sonnet']['latency_s']}s | "
              f"open ${job['open']['cost_usd']:.4f} {job['open']['latency_s']}s")
        return job

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        records = list(pool.map(run, jobs))

    json_path = out_dir / f"summary_models_{stamp}.json"
    json_path.write_text(json.dumps(records, indent=1, ensure_ascii=False, default=str), encoding="utf-8")
    md_path = out_dir / f"summary_models_{stamp}.md"
    write_report(records, md_path)
    print(f"wrote {json_path} and {md_path}")
    print(f"sonnet total ${sum(r['sonnet']['cost_usd'] for r in records):.3f}, "
          f"open total ${sum(r['open']['cost_usd'] for r in records):.3f}")


if __name__ == "__main__":
    main()

# Can an open-weight model take the investment-pack summaries? (2026-09-12)

**Answer: not for the money on offer.** GLM-5.2 is the only open model of
five tried that matches Sonnet 5's coverage; it also made three small
factual errors in twenty documents where Sonnet made none, to save about
$2.30 a month. The summariser stays on Sonnet 5.

## What was run

`scripts/eval_summary_models.py`: the production summary prompt
(`summarizer.SYSTEM_PROMPT` + `build_extraction_prompt`, same
`smart_truncate` text) on the twenty most recent documents that
`choose_model` routes to Sonnet — at most two per plan, duplicates by text
hash removed. Sonnet 5 ran once, synchronously, with thinking disabled as
in production ($1.24). Each open model then ran against the saved Sonnet
outputs through OpenRouter with reasoning off, which is the production
parity setting and also what stops the empty outputs described below.

Reports and every raw response are in the session scratchpad
(`eval_out/summary_models_20260912*.{json,md}`); the JSON files carry the
full prompts, so they are not committed.

## Results

Item counts are relative to Sonnet 5 across the same twenty documents
(Sonnet: 79 decisions, 37 investment actions, 129 performance rows, 165
notable items). "Grounded" is the share of extracted return/benchmark
percentages that appear verbatim in the source text.

| Model | 20 docs | Latency median / max | Failures | Decisions | Actions | Perf rows | Notable | Grounded |
|---|---|---|---|---|---|---|---|---|
| Sonnet 5 (thinking off) | $1.24 | 16s / 26s | 0 | 1.00 | 1.00 | 1.00 | 1.00 | 99% |
| DeepSeek V4 Flash, reasoning on | $0.03 | 27s / 167s | 2 empty | 0.94 | 0.58 | 0.61 | 0.67 | 97% |
| DeepSeek V4 Flash, reasoning off | $0.03 | 13s / 104s | 0 | 0.82 | 0.51 | 0.50 | 0.72 | 99% |
| DeepSeek V4 Pro (0813), off | $0.20 | 15s / 29s | 0 | 0.72 | 0.38 | 0.70 | 0.79 | 100% |
| Qwen 3.7 Plus, off | $0.12 | 12s / 18s | 0 | 0.89 | 0.43 | 0.42 | 0.53 | 96% |
| GLM-5.2, off | $0.22 | 17s / 26s | 0 | 1.08 | 0.95 | 1.06 | 0.99 | 98% |

The two Flash "empty" results were the model spending the whole
6,000-token budget on reasoning and returning no content at
`finish_reason=length`. Reasoning off cures that and makes it thinner.

## Spot checks against the source

- **El Paso, July 2026 minutes.** The rebalancing motion was polled with
  six ayes. Sonnet and the stored production summary say 6-0. Every open
  model says 5-0 or "unanimous"; GLM-5.2 says 5-0 (the consent-agenda
  count, reused).
- **North Dakota SIB, September 2026 pack.** Sonnet names the terminated
  mandate (William Blair International Leaders, Feb 2026) and the four
  additions with their months. GLM-5.2 lists the additions but dates
  Principal International to March where the pack says February, and
  records "terminated manager" without the name. DeepSeek Pro and Qwen
  found no manager changes at all.
- **Arkansas TRS experience study.** Four capital-market return figures in
  the pack. Sonnet 4, GLM 3, DeepSeek Pro 1, Flash and Qwen 0.
- **Virginia RS DCPAC.** BlackRock glidepath change and fee cut: Sonnet 3
  actions, GLM 2, Flash-off 2, the rest 0.
- **HPOPS commitments ($30M Franklin Park, $60M HarbourVest).** Every model
  got both amounts except Flash-off, which dropped the $30M.
- **TA Realty co-portfolio-manager change (El Paso)**, the kind of item the
  Weekly Watch exists for: Sonnet, GLM and Qwen carried it; both DeepSeeks
  dropped it.

Nothing any model produced was found to be invented; the failures are
omissions and small transcription slips, not fabrication.

## The money

Production summaries run as a Message Batch at half price, so Sonnet's
twenty documents cost about $0.62 there, against GLM-5.2's $0.22 at list
(its `:batch` variant is priced higher than its standard one). The
summarise line is about $3.50 a month on Sonnet; GLM would make it about
$1.20. That is the whole prize: roughly $2.30 a month.

## Decision

Keep Sonnet 5. The saving does not cover the risk of a wrong vote count
or a misdated manager change propagating into the digest, the briefings
and the Performance tab. Revisit if either the document volume grows by an
order of magnitude or a later GLM/DeepSeek release closes the gap; the
script reruns in a few minutes for about $0.25 with `--reuse-sonnet`.

The `costs.PRICES` rows for the three candidates stay so the script keeps
working; nothing scheduled uses them.

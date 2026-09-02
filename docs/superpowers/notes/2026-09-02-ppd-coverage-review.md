# Coverage review: the registry against the Public Plans Database

**2026-09-02.** Answers D15. Reproduce with `python -m scripts.ppd_coverage`
(add `--states` for the per-state side-by-side this note was read off).

## What was compared, and why not "the top 200"

The request was a top-200 review. The phrase usually means *Pensions &
Investments*' Top 200, which is mostly **corporate** DB plans — GM, Boeing,
IBM. This project tracks US **public** plans, so that ranking would score us
against plans we deliberately do not cover and should not add.

The benchmark used instead is the **Public Plans Database** (Center for
Retirement Research at Boston College): 248 state and local plans holding
~95% of US state/local pension assets, refreshed to FY2025 for 186 of them.
That is the universe this registry is trying to be a subset of.

| | plans | assets |
|---|---|---|
| PPD universe | 248 | $5,856B |
| `data/known_plans.json` | 148 | $5,689B |

**Every state and DC has at least one tracked plan.** There is no state-shaped
hole.

## Why plan-name matching cannot answer this

PPD's unit is the pension plan. Ours is the entity that invests the money. Six
PPD rows named "Washington … Plan 1/2/3" are one WSIB portfolio; "NY State &
Local ERS" and "NY State & Local Police & Fire" are both the NY Common
Retirement Fund. Any name matcher reports these as gaps.

Two attempts are in git history and both failed in ways that would have set
work in motion:

- Stripping boilerplate (`employees`, `state`, `retirement`) deleted the words
  that carry the meaning, and reported four tracked Houston and Austin funds as
  missing.
- Stripping the state name instead scored **"University of California" as
  covered by CalPERS**, on the single shared word "California" — $110.8B hidden
  by a tokeniser.

So `scripts/ppd_coverage.py` deliberately does not classify. It prints state
totals — the one comparison immune to roll-up, since both sides sum the same
money — and the side-by-side listing. The conclusions below were read by hand.

## Finding 1: one large genuine gap

**The University of California Retirement Plan, $110.8B.** It would rank #11
in the country, above Oregon PERS and Arizona SRS, both of which we track. No
`regents` or `university of california` entry exists in the registry. It is the
only omission at that scale, and the only one that changes the shape of the
corpus.

## Finding 2: the rest of the gap is municipal, and long-tailed

Roughly **$60B across ~50 small funds**, none individually above $12B. The
ones worth considering, largest first:

| $B | plan | note |
|---|---|---|
| 11.9 | Arkansas PERS | we track only Arkansas Teachers |
| 12.5 | Chicago Municipal, Police, Fire, Laborers | we track Chicago Teachers and Cook County |
| 9.5 | Louisiana Parochial, Schools, Municipal | four other Louisiana funds tracked |
| 8.0 | Kansas City + St. Louis municipal funds (11 funds) | none tracked |
| 4.1 | Missouri DOT and Highway | separate from MOSERS |
| 3.8 | Connecticut Municipal | |
| 3.7 | Arkansas Police and Fire | |
| 3.6 | Arlington County (VA) | Fairfax is tracked |
| 3.5 | Baltimore Fire and Police | Baltimore City ERS is tracked |
| 4.0 | Pennsylvania Municipal | |

Below that it is city police-and-fire funds of $0.1–1.5B: Omaha, Wichita,
Tucson, Hartford, Birmingham, Miami, Jacksonville, Atlanta Police, St. Paul
Teachers, Baton Rouge, Lexington-Fayette, Providence, Sioux Falls, and so on.

**These are cheap to add and cheap to skip.** Each is one registry entry plus a
`materials_url`, and each moves tracked AUM by a fraction of a percent. The
argument for adding them is completeness of the *board-materials* corpus, not
assets — a small city fund still publishes minutes and manager searches.

## Finding 3: `aum_billions` is systematically ~12% low

This one was not asked for and matters more than the second finding.

Across **32 hand-verified pairs**, the median ratio of our figure to PPD's
reported market value is **0.881**. Twenty-six of the 32 are understated, three
are within 3%, and three are overstated. That is not rounding — it is a vintage
lag, and it is one-directional.

| plan | ours | PPD | |
|---|---|---|---|
| Philadelphia | $6.0B | $9.5B | −37% |
| Connecticut Teachers | $19.0B | $29.3B | −35% |
| Arizona Public Safety | $12.0B | $18.2B | −34% |
| New Hampshire RS | $9.0B | $13.4B | −33% |
| CalPERS | $502B | $563B | −11% |
| Texas Teachers | $200B | $226B | −11% |

**Where this bites:** `CLAUDE.md` states the WAF-blocked plans are "8.5% of
tracked AUM". That percentage is computed from this column, so it inherits the
error — and unevenly, because the lag differs per plan. Any AUM-weighted claim
in the docs should be read as approximate until the column is refreshed.

Refreshing it is now easy and free: PPD gives a market value per plan, and
`scripts/ppd_coverage.py` already fetches it. The obstacle is the same name
matching this note argues against automating, so it wants a one-time
hand-checked map from `plan_id` to `ppd_id`, stored in the registry. Once that
map exists the figure can be refreshed on every run.

## Six states where tracked assets are notably short

Read `--states` for these before adding anything: CT 64%, AR 51%, LA 67%,
SC 69%, OK 73%, NE 57%. In each case the shortfall is a mix of Finding 2 and
Finding 3, and the split differs by state — South Carolina's gap is almost
entirely the stale AUM figure (PEBA is tracked and covers both plans), whereas
Arkansas's is a genuinely missing $11.9B fund.

Six states run the other way, where our total exceeds PPD's because we record
an investment board that holds more than its DB pension plans: MN, NY, WA, MA,
OR, WV. Not errors, and not extra coverage.

## Recommendation

1. **Add the University of California Retirement Plan.** One entry, $110.8B,
   and the only omission that changes what the corpus is.
2. **Build the `plan_id` → `ppd_id` map and refresh `aum_billions`.** Fixes a
   systematic 12% understatement and makes every AUM-weighted claim in the docs
   true rather than approximately true.
3. **Leave the municipal long tail alone unless board materials are the goal.**
   Fifty entries for ~1% of assets is only worth it if the aim is document
   coverage rather than asset coverage — which is a question for James, not a
   default.

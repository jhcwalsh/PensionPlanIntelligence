# Macro Agent — Design Document

**Pipeline:** Agentic Strategic Asset Allocation
**Stage:** 1 of 6 (precedes Asset Class agents)
**Version:** v0.1 (draft for iteration)
**Status:** Design — not yet implemented

---

## 0. Purpose and Scope

The macro agent is the first stage of the agentic SAA pipeline described in Ang, Azimbayev & Kim (2026). Its job is to classify the current macroeconomic regime, produce a probability distribution over four regimes, and emit a structured `macro-view.json` that conditions every downstream agent (asset-class CMA agents, covariance estimation, portfolio construction).

Key design commitments:

1. **Output is a posterior distribution, not a hard label.** Most regime-switching literature (Ang & Timmermann; Guidolin & Timmermann) shows the value is in the probability mass and the transition matrix, not the argmax.
2. **Regimes are defined on growth × inflation direction**, not cycle position. Both this design and Ang, Azimbayev & Kim (2026) use four regimes; the difference is the axes. The paper uses cycle-position labels (recovery, expansion, late-cycle, recession) with an implicit temporal ordering through the business cycle. We use two independent direction-of-change axes (growth and inflation), with no implied ordering — any quadrant can follow any other. Rationale: the 2D state space maps more cleanly onto cross-asset risk premia, and stagflation has no clean home in the cycle-position taxonomy (it typically gets absorbed into "late-cycle"), which is the exact gap the paper's own March 2026 example surfaces.
3. **Multiple methods, structured voting, LLM-as-judge.** Mirrors the paper's portfolio-construction strategy review (Section 3.5) and CIO architecture (Section 3.6).
4. **The Investment Policy Statement (IPS) is read at runtime** and can override or constrain regime calls (e.g., if the IPS bars stagflation hedges, the agent must surface a flag rather than silently apply them downstream).

---

## 1. Regime Taxonomy

Four regimes defined by the joint state of growth and inflation, each measured as a *change relative to expectations and trend* — not absolute levels.

| Regime | Growth | Inflation | Canonical asset behavior |
|---|---|---|---|
| **Recovery** | ↑ | ↓ or stable | Risk-on, duration friendly, credit spreads tighten |
| **Expansion** | ↑ | ↑ or stable | Equities & commodities up, bonds neutral-to-down |
| **Stagflation** | ↓ | ↑ | Real assets / gold lead, stock-bond correlation flips positive |
| **Recession** | ↓ | ↓ | Duration leads, credit and equities sell off |

**Reconciliation with the paper's taxonomy.** Both schemes have four regimes; they differ in their defining axes, not their cardinality.

| | Paper (Ang/Azimbayev/Kim 2026) | This design |
|---|---|---|
| Axes | Cycle position (single ordered axis) | Growth direction × inflation direction (two independent axes) |
| Regimes | Recovery → Expansion → Late-cycle → Recession | Recovery, Expansion, Stagflation, Recession |
| Temporal structure | Implicit ordering (regimes typically follow a sequence) | None — any quadrant can follow any other |
| Stagflation | No dedicated regime; absorbed into "late-cycle" | First-class regime |
| Disinflationary recession | Mapped to "recession" | Mapped to "recession" (low growth, low inflation) |
| Stagflationary recession | Mapped to "recession" | Mapped to "stagflation" (low growth, high inflation) |

Approximate but imperfect mapping: Recovery ↔ Recovery; Expansion ↔ Expansion; Late-cycle splits between Expansion and Stagflation depending on inflation prints; Recession splits between Recession and Stagflation depending on inflation prints. The paper's March 2026 example — late-cycle with stagflationary risk — illustrates precisely the case where the cycle-position label is underspecified and the growth × inflation framing is more informative.

**Outputs are always probabilistic.** A late-cycle environment with stagflationary risk might be reported as `{recovery: 0.05, expansion: 0.30, stagflation: 0.55, recession: 0.10}` rather than a binary label.

---

## 2. Approaches

The macro agent orchestrates nine sub-method agents organized into five families. Each sub-method produces an identical structured output and they are voted across in Section 4.

### Family A — Deterministic / scoring

**A1. Rule-based scoring.** Weighted z-scores on growth, inflation, monetary policy, and financial conditions dimensions, mapped through threshold rules to regime probabilities. Most interpretable method; baseline anchor in the Self-Driving Portfolio paper. Weak at turning points.

### Family B — Statistical / econometric

**B1. Hidden Markov Model (Hamilton 1989; Ang & Timmermann).** Two-to-four state HMM fit jointly on real GDP growth, core PCE, and a financial conditions composite. Output: smoothed regime probabilities and the transition matrix. The transition matrix is independently valuable for downstream Black-Litterman.

**B2. Markov-switching VAR (Ang & Timmermann).** Multivariate extension capturing joint dynamics across growth, inflation, and a financial conditions block. Captures fat tails, time-varying correlations, and heteroskedasticity that single-state models miss.

**B3. Macro factor extraction (Ludvigson & Ng 2009).** Extracts two latent factors — a real-activity factor and a price-pressure factor — from a FRED-MD style panel of 100+ series. Then classifies the (F_growth, F_inflation) state. Dominates A1 when underlying signals are noisy or redundant.

### Family C — Machine learning / data-driven

**C1. PCA + k-means clustering (Akioyamen, Tang & Hussien 2020).** Dimensionality-reduce the macro panel, cluster historical periods into k=4 latent regimes, assign current point to nearest cluster. Unsupervised — useful sanity check on labeled methods.

**C2. DCC-GARCH correlation regimes (Ryu).** Classifies on the cross-asset correlation structure, not macro data. Stagflation regimes are uniquely identifiable by positive stock-bond correlation; recoveries by collapsing credit spreads. Market-implied, leads official data.

**C3. Supervised ML classifier (Thomson 2025).** Gradient-boosted classifier trained on labeled historical regimes (NBER + inflation-direction overlay). Calibrated probabilistic output; handles non-linear interactions.

### Family D — Nowcasting

**D1. Nowcast threshold rule.** Pulls real-time nowcasts (Atlanta Fed GDPNow, NY Fed WEI, Cleveland Fed inflation nowcast) and applies a 2×2 threshold grid. Fastest-updating method; critical for turning points before quarterly data confirms.

### Family E — Soft / textual

**E1. LLM-as-judge with web evidence.** Reads FOMC statements, ECB speeches, broker commentary, financial press. Outputs a regime call grounded in narrative evidence. Cochrane's "Financial Markets and the Real Economy" frames why "bad times" is partly interpretive.

### Method comparison summary

| Method | Family | Strength | Weakness | Update cadence |
|---|---|---|---|---|
| A1 Rule-based | Deterministic | Interpretable, auditable | Weak at turns | Monthly |
| B1 HMM | Statistical | Transition matrix | Few states, slow | Monthly |
| B2 MS-VAR | Statistical | Joint dynamics, vol | Estimation-heavy | Monthly |
| B3 Macro factors | Statistical | Robust to noise | Black-box loadings | Monthly |
| C1 PCA+k-means | ML unsupervised | No labels needed | Cluster instability | Weekly |
| C2 DCC-GARCH | ML / market | Leading indicator | Pure market view | Daily |
| C3 Supervised ML | ML | Calibrated probs | Label dependency | Monthly |
| D1 Nowcast | Nowcasting | Fastest updates | High noise | Weekly |
| E1 LLM-textual | Soft | Captures narrative | Hardest to audit | Daily |

---

## 3. Signals to Watch

Organized by the four scoring dimensions (growth, inflation, monetary policy / financial conditions), plus a fifth set of regime-specific telltales that disambiguate adjacent quadrants.

### 3.1 Growth dimension

| Category | Signals |
|---|---|
| Real activity (coincident) | Real GDP, GDP+, NY Fed Weekly Economic Index, industrial production, real personal income ex-transfers, real consumer spending, aggregate hours worked |
| Labor | Nonfarm payrolls (3m avg), initial claims (4w avg), unemployment rate vs Sahm rule, labor force participation, JOLTS quits & openings |
| Surveys (diffusion) | ISM Manufacturing, ISM Services, S&P Global PMIs, Conference Board LEI, NFIB small business, regional Fed surveys (Empire, Philly, Dallas, KC, Richmond, Chicago) |
| Forward-looking | Yield curve slope (10y-3m, 10y-2y), term premium decomposition, building permits, durable-goods new orders ex-air, capex orders |
| Global cross-check | OECD composite leading indicators, China Caixin PMI, Eurozone composite PMI, Korean exports |

### 3.2 Inflation dimension

| Category | Signals |
|---|---|
| Headline / core | CPI, core CPI, supercore (services ex-housing), PCE, core PCE |
| Robust measures | Cleveland Fed trimmed-mean PCE, median CPI, Atlanta Fed sticky CPI, NY Fed underlying inflation gauge |
| Wages | Employment Cost Index, Atlanta Fed Wage Growth Tracker, average hourly earnings (production & nonsupervisory) |
| Expectations | 5y5y inflation breakeven, TIPS-implied at multiple horizons, NY Fed Survey of Consumer Expectations, U Mich expectations, SPF, Coibion-Gorodnichenko firm survey |
| Inputs / pipeline | Brent & WTI crude, industrial metals (LME), Bloomberg commodity index, dollar index, NY Fed Global Supply Chain Pressure Index, container freight rates |

### 3.3 Monetary policy & financial conditions

| Category | Signals |
|---|---|
| Policy stance | Fed funds rate, real fed funds (vs core PCE), shadow rate (Wu-Xia / Krippner) when at ZLB, OIS-implied path 12m forward |
| Real yields | 5y and 10y TIPS yields, real curve slope |
| Financial conditions | Chicago Fed NFCI (and subindices: risk, credit, leverage, nonfinancial), Bloomberg US FCI, GS FCI |
| Credit / risk | IG and HY OAS, CDX IG/HY, MOVE index, VIX, VIX term structure |
| Bank channel | Senior Loan Officer Opinion Survey (SLOOS) — willingness to lend, C&I tightening, household demand |

### 3.4 Regime-specific telltales

These don't classify by themselves but materially shift posteriors when they break out of normal ranges.

| Signal | What it tells us | Theoretical basis |
|---|---|---|
| Realized stock-bond correlation (rolling 6m) | Positive correlation is a near-unique fingerprint of stagflation/policy-shock regimes | Campbell-Vuolteenaho cash-flow vs discount-rate decomposition |
| Gold real-rate beta | Decoupling of gold from real rates often precedes regime change | Real-asset hedging premium |
| Cyclical vs defensive equity ratio | Cyclical leadership in falling-yield environments → recovery; defensive leadership in rising-yield environments → stagflation | Sector composition signal |
| Curve dynamics | Bull-steepening → recession/recovery; bear-steepening → stagflation; bear-flattening → late expansion | Expectations + term-premium decomposition |
| Credit-equity divergence | Credit leading equity down typically precedes regime change to recession | Credit market efficiency in distress |

---

## 4. Orchestrated Voting Methodology

This mirrors the paper's PC Strategy Review (Section 3.5) adapted for classification. Six stages.

### Stage 1 — Parallel method execution

All nine sub-method agents run in parallel. They share a single data-fetch script that populates a common cache, so disagreement reflects methodology, not stale inputs.

Each method emits a structured output conforming to a schema:

```json
{
  "method_id": "B1_HMM",
  "regime_probs": {"recovery": 0.10, "expansion": 0.20, "stagflation": 0.55, "recession": 0.15},
  "growth_z": -0.8,
  "inflation_z": 1.2,
  "confidence": 0.65,
  "key_drivers": ["payrolls negative Feb", "Brent +18% on Iran shock", "CPI re-accelerating"],
  "narrative": "..."
}
```

### Stage 2 — Diagnostic agent (CRO-equivalent)

A neutral diagnostic agent computes:

- **Method dispersion.** Entropy of the average distribution, max pairwise KL divergence between methods, standard deviation of growth and inflation z-scores across methods.
- **Trailing accuracy weights.** Each method's hit rate over the past 12 quarters — specifically, the probability mass it assigned to the regime retrospectively determined to be correct (NBER + inflation-direction labels with one-year lag).
- **Regime-specific accuracy.** Conditional accuracy by regime — methods accurate in stable regimes are often poor at turning points and vice versa.

The diagnostic agent does not vote. It produces a report that all voting agents read.

### Stage 3 — Peer review

Following the paper's intra/inter-category pattern, each method agent reviews two others — one in its own family (technical critique within shared assumptions) and one from a different family (foundational challenge from a contrasting worldview). Assignments are randomized with a recorded seed.

Reviews focus on three questions:

1. Are the inputs stale or revised since the run started?
2. Does the agent's narrative match its numerical output?
3. Is the confidence score consistent with the method's dispersion vs the diagnostic baseline?

Each method agent may revise its output once based on peer comments.

### Stage 4 — Three-layer aggregation

Three candidate aggregations are produced; the Chief Economist agent in Stage 5 judges between them.

**4a. Hard Borda vote.** Each method submits a ranking of the four regimes (3-2-1-0 points) weighted by trailing accuracy. Most legible to human reviewers; produces a clean argmax regime.

**4b. Soft probabilistic blend (linear opinion pool).** Final probability for regime *r* = accuracy-weighted average of each method's probability for *r*. Weights are **regime-conditional on the previous-period view**: during transitions (high dispersion), nowcasting and Markov-switching methods are up-weighted; during stable regimes, rule-based and supervised ML are up-weighted.

**4c. Logarithmic pool.** Multiplies probabilities in log space rather than averaging. Sharper, more confident output — useful when methods agree, dangerous when they don't. Included as a diversity check on 4b.

### Stage 5 — Chief Economist LLM-as-judge

Macro analog of the paper's CIO agent. Receives the nine method outputs, peer reviews, diagnostic report, and three candidate aggregations. Applies decision rules conditioned on dispersion and trailing accuracy:

| Condition | Decision rule |
|---|---|
| Low dispersion, all methods agree | Accept the logarithmic pool (4c) — sharpens consensus |
| Moderate dispersion | Accept the soft blend (4b) |
| High dispersion or regime transition signal | Tilt toward Markov-switching + nowcasting; flag the call as low-confidence |
| One method is a clear outlier with low trailing accuracy | Down-weight or exclude |
| Methods agree on growth but disagree on inflation (or vice versa) | Report split confidence: high on one axis, low on the other |

**Hard constraint** (analog to the CMA Judge in Exhibit 4 of the paper): the final probability distribution must lie within the convex hull of the method outputs. The judge cannot invent a regime probability higher than any input method assigned.

**Diversity constraint:** the final blend must give non-zero weight to at least two of the five families.

### Stage 6 — Meta-agent feedback loop

After each rebalancing cycle (typically quarterly), the meta-agent (per Section 5.3 of the paper) compares the regime call against realized macro outcomes with the natural 1–2 quarter NBER-style lag. It updates:

- Trailing accuracy weights used in Stage 2
- Signal-to-dimension mapping in the rule-based scoring (A1)
- Features and class labels for the supervised ML method (C3)
- Threshold values in the nowcast rule (D1)

It can also propose new methods via a researcher-agent analog — for example, an information-theoretic regime detector, or a regime-switching factor model in the spirit of Guidolin & Timmermann that integrates skew and kurtosis preferences directly.

---

## 5. Output Contract

The `macro-view.json` consumed by every downstream agent:

```json
{
  "run_id": "macro_2026_03_15_001",
  "as_of_date": "2026-03-15",
  "primary_regime": "stagflation",
  "regime_probs": {
    "recovery": 0.05,
    "expansion": 0.30,
    "stagflation": 0.55,
    "recession": 0.10
  },
  "growth_z": -0.7,
  "inflation_z": 1.0,
  "confidence": "medium",
  "transition_probs": {
    "from_current_to": {
      "recovery": 0.02,
      "expansion": 0.10,
      "stagflation": 0.78,
      "recession": 0.10
    },
    "horizon_months": 3
  },
  "flip_triggers": [
    "core PCE prints below 2.4% for 2 consecutive months",
    "payrolls return to +150k 3m avg",
    "Brent below $75 for 30 days"
  ],
  "ips_compliance": {
    "passes": true,
    "flags": []
  },
  "narrative_for_cio_memo": "...",
  "method_audit_trail": [
    {
      "method_id": "B1_HMM",
      "weight_in_final_blend": 0.18,
      "vote": "stagflation",
      "confidence": 0.65,
      "rationale": "..."
    }
  ]
}
```

The transition matrix is exposed in full (not just `from_current_to`) so that Black-Litterman and regime-conditional CMA methods downstream can set conditional expected returns.

---

## 6. Agent Configuration (mirrors paper Exhibit 3 style)

```
Macro Agent — Regime Classification
Role: Classify current macroeconomic regime across growth × inflation
      and emit macro-view.json consumed by all downstream agents
Slug: macro-agent | Category: Pipeline Stage 1 | History: Jan 1990–present
Required skills: macro-data-fetch, regime-classification, hmm-toolkit,
                 dcc-garch, nowcast-ingest, fred-md-factors, llm-judge,
                 web-search-macro
Workflow:
1. Read IPS for any regime-classification constraints or flags
2. Fetch macro panel via data API (FRED, BLS, BEA, OECD) + market data
3. Web search for real-time signals (FOMC, oil shocks, geopolitical)
4. Run 9 sub-method agents in parallel (A1, B1-B3, C1-C3, D1, E1)
5. Diagnostic agent computes dispersion, accuracy weights, regime-conditional accuracy
6. Peer review: each method reviews 2 others (1 intra-family, 1 inter-family)
7. Methods revise outputs based on peer comments
8. Three candidate aggregations: Borda, soft blend, log pool
9. Chief Economist LLM-as-judge selects/combines, applies diversity constraint
10. Emit macro-view.json + narrative report
Key considerations (read at runtime):
• Output must be probability distribution, not hard label
• Final distribution must lie in convex hull of method outputs (hard constraint)
• At least 2 families must contribute non-zero weight (diversity constraint)
• Flag turning-point risk (high dispersion) prominently
• Expose full transition matrix for downstream BL conditioning
• IPS overrides apply BEFORE final output is emitted
Output: macro-view.json, methods_audit.json, diagnostic_report.json,
        peer_reviews.json, analysis.md
```

---

## 7. Open Design Questions

Flagged for resolution before or during implementation.

**Q1. Labeling scheme for supervised ML (C3).** NBER gives clean recession labels. The inflation overlay is judgment-heavy. Thomson (2025) uses a "durable vs fragile" binary that bypasses the four-regime structure. **Recommendation:** pilot both labeling schemes and evaluate holdout stability.

**Q2. How to handle the "soft" LLM-textual method (E1) in voting.** It can't be backtested cleanly (lookahead-bias problem flagged in Section 5.1 of the paper). **Recommendation:** include E1 in the soft blend (4b) at fixed low weight; exclude from accuracy-weighted aggregation; use primarily as a flip-trigger detector.

**Q3. Regime persistence vs reactivity tradeoff.** If we re-classify weekly, we risk regime whiplash. If we re-classify only monthly, we miss real transitions. **Recommendation:** weekly probability updates, but the *primary_regime* label only flips when the new regime exceeds 0.55 probability for two consecutive weeks.

**Q4. Treatment of stock-bond correlation as input vs validation.** The correlation regime is both a feature (C2 uses it) and a downstream validation signal. **Recommendation:** allow C2 to use it as a feature, but the diagnostic agent in Stage 2 also reports it as a cross-check on the final output.

**Q5. Global vs US-only regime classification.** Currently all signals are US-centric. For a true multi-asset portfolio with international exposure, do we need regional regime calls? **Recommendation:** start US-only for v1; add a secondary "global" regime track in v2 once US version is stable.

**Q6. Computational cost of B2 (MS-VAR) and C3 (gradient-boosted ML).** Both require re-estimation each run. **Recommendation:** monthly full re-estimation; weekly inference-only updates using cached parameters.

---

## 8. References

### Self-Driving Portfolio paper (primary)

- Ang, Andrew, Nazym Azimbayev, and Andrey Kim. 2026. "The Self-Driving Portfolio: Agentic Architecture for Institutional Asset Management." Working paper.

### Regime-switching literature (Zotero collection "Regime Shifts")

- Ang, Andrew, and Allan Timmermann. NBER Working Paper. "Regime Changes and Financial Markets." Foundational survey; informs B1 (HMM) and B2 (MS-VAR).
- Guidolin, Massimo, and Allan Timmermann. "Optimal Portfolio Choice under Regime Switching, Skew and Kurtosis Preferences." Multi-period asset allocation with regimes; informs the use of transition matrices downstream.
- Hamilton, James D. 1989. (Implicit) "A New Approach to the Economic Analysis of Nonstationary Time Series and the Business Cycle." Foundational HMM in macro.
- Ludvigson, Sydney C., and Serena Ng. 2009. "Macro Factors in Bond Risk Premia." Review of Financial Studies. Informs B3 (macro factor extraction).
- Akioyamen, Peter, Yi Zhou Tang, and Hussien Hussien. 2020. "A Hybrid Learning Approach to Detecting Regime Switches in Financial Markets." Informs C1 (PCA + k-means).
- Ryu, Jehyeon. "Asset Allocation Under Shifting Correlations." Informs C2 (DCC-GARCH correlation regimes).
- Thomson, Alan D. 2025. "Enhancing Portfolio Efficiency: A Machine Learning Approach to Regime Classification." Journal of Financial Data Science. Informs C3 (supervised ML).
- Cochrane, John H. "Financial Markets and the Real Economy." Frames E1 (textual / interpretive regime classification).
- Campbell, John Y., and Tuomo Vuolteenaho. 2004. "Bad Beta, Good Beta." American Economic Review. Cash-flow vs discount-rate decomposition; theoretical basis for stock-bond correlation telltale.
- Smets, Frank, and Rafael Wouters. "Shocks and frictions in US business cycles: a Bayesian DSGE approach." Informs the structural interpretation of macro shocks.

### Multi-agent / LLM literature (paper bibliography)

- Du et al. 2023; Chen et al. 2025; Chuang et al. 2024 — multi-agent debate and voting.
- Zheng et al. 2023 — LLM-as-judge methodology.

---

## Changelog

- **v0.2** — Clarified that both the paper's taxonomy and this design use four regimes; the difference is the defining axes (cycle position vs growth × inflation direction), not the cardinality. Added explicit mapping table in Section 1.
- **v0.1** — Initial draft. Nine-method architecture, six-stage voting pipeline, JSON output contract. Open questions Q1–Q6 flagged for iteration.

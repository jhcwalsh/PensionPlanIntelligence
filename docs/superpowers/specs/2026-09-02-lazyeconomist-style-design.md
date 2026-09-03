# Restyling the Streamlit app to match lazyeconomist.com

**2026-09-02. Implemented 2026-09-03.** Requested by James. See "As built" at
the end for what the spec got right, what it missed, and the one thing it
should have warned about and did not.

## The finding that shapes this

**Almost all of it is supported configuration, not CSS hacks.** Streamlit
1.58 (pinned `>=1.52`, 1.58.0 installed) exposes `theme.font`,
`theme.headingFont`, `theme.codeFont`, `theme.fontFaces`, `theme.baseRadius`,
`theme.borderColor`, `theme.linkColor`, `theme.headingFontSizes`,
`theme.headingFontWeights`, `theme.chartCategoricalColors` and
`theme.dataframeHeaderBackgroundColor`, each with a `theme.sidebar.*` and
`theme.dark.*` counterpart.

That matters because the obvious way to restyle Streamlit — inject CSS at
`.st-emotion-cache-1r6slb0` — breaks on every upgrade, silently, and looks
fine in review. This design uses `.streamlit/config.toml` for everything it
can, and confines CSS to a short list of `data-testid` selectors, which are
part of Streamlit's testing contract and change far less often.

## Where the app is now

`app.py:42-72`: `set_page_config(layout="wide")` plus 22 lines of CSS defining
three classes — `.summary-card` (Material blue `#0066cc` left border on
`#f8f9fa`), `.tag`, `.action-tag`. Everything else is stock Streamlit: white
background, Source Sans, `#ff4b4b` primary. There is no `.streamlit/`
directory. One chart (`st.altair_chart`), many `st.dataframe`, ten tabs.

## The target, read off the source

lazyeconomist.com serves its whole design system in one inline `<style>`
block. Extracted verbatim:

```
--bg:          #fbfaf7   warm off-white, not white
--bg-soft:     #f4f2ec   secondary surface
--ink:         #1a1a1a   near-black
--ink-soft:    #4a4a4a   body
--ink-faint:   #8a8780   metadata; warm grey, not neutral
--rule:        #e8e4dc   hairline
--accent:      #b8410e   burnt orange, single accent
--accent-soft: #f5e6dd   accent tint
```

Three fonts, three jobs:

| face | role | treatment |
|---|---|---|
| **Fraunces** (serif, variable optical size) | headings, card titles | weight 400–500, `letter-spacing: -0.02em` to `-0.025em`, `clamp(32px, 4vw, 44px)` for h2 |
| **Inter Tight** | body, UI | 15–16px, `line-height: 1.5`, `font-feature-settings: 'ss01','cv11'` |
| **JetBrains Mono** | eyebrow labels, tags, status | 11–12px, `text-transform: uppercase`, `letter-spacing: 0.1em`–`0.12em`, always `--ink-faint` |

Character: **editorial, warm, quiet**. Paper-toned background rather than
white; one accent used sparingly; hairline rules doing the work borders and
shadows usually do; generous space (100px section padding, 36px/32px cards);
square corners on containers with fully-round pills (`border-radius: 999px`)
for status chips. Cards are a grid with `gap: 1px` over a `--rule` background,
so the dividers *are* the gap — no per-card borders.

## What maps, and how

### `.streamlit/config.toml` — new file, does most of the work

```toml
[theme]
base                = "light"
backgroundColor     = "#fbfaf7"
secondaryBackgroundColor = "#f4f2ec"
textColor           = "#1a1a1a"
primaryColor        = "#b8410e"
borderColor         = "#e8e4dc"
linkColor           = "#b8410e"
dataframeHeaderBackgroundColor = "#f4f2ec"
baseRadius          = "2px"          # square containers; pills are CSS
font                = "Inter Tight"
headingFont         = "Fraunces"
codeFont            = "JetBrains Mono"
headingFontWeights  = [400, 400, 500, 500, 600, 600]
headingFontSizes    = ["2.5rem", "1.875rem", "1.375rem", "1.125rem", "1rem", "0.875rem"]
chartCategoricalColors = [
  "#b8410e", "#8a8780", "#3d5a5c", "#c98a5e",
  "#5c6b4a", "#7a5c6b", "#4a4a4a", "#d4b483",
]
```

**The `fontFaces` gotcha, which will cost an hour if unrecorded.**
`[[theme.fontFaces]]` takes a `url` pointing at an actual font *file*
(`.woff2`), not at the `fonts.googleapis.com/css2?family=…` stylesheet the
site uses. Passing the stylesheet URL fails quietly — the font simply does not
apply, and `font = "Fraunces"` falls back. Two options:

1. Resolve each `css2` URL to its `fonts.gstatic.com/s/...woff2` files and
   list one `[[theme.fontFaces]]` per family/weight. Free, but the URLs are
   opaque hashes that Google may rotate.
2. Vendor the `.woff2` files into `static/fonts/`, set
   `server.enableStaticServing = true`, and point `url` at
   `app/static/fonts/…`. Stable, self-hosted, and removes a third-party
   request from a page that currently only calls out to Plausible.

**Recommend option 2.** It is three files (Fraunces variable, Inter Tight,
JetBrains Mono), it cannot break when Google rotates a URL, and it keeps
render deterministic — which matters more here than the ~200KB, because the
app already loads multi-megabyte dataframes.

### CSS that config cannot express

Replaces the current block in `app.py`. Every selector below is either a
plain HTML element inside our own markdown or a `data-testid`:

1. **`.summary-card`** — drop the Material blue left border for a hairline
   box: `background: #fbfaf7; border: 1px solid #e8e4dc; border-radius: 2px;
   padding: 20px 24px`. Left-accent bars are a Bootstrap idiom and read as
   foreign here.
2. **`.tag` / `.action-tag`** — become the mono pill:
   `font-family: 'JetBrains Mono'; font-size: 11px; text-transform: uppercase;
   letter-spacing: 0.1em; border-radius: 999px; padding: 3px 10px`. `.tag` on
   `--bg-soft`/`--ink-faint` with a `--rule` border; `.action-tag` on
   `--accent-soft`/`--accent`, borderless. This is lazyeconomist's
   `.app-status` / `.app-status.live` pair exactly.
3. **`[data-testid="stCaptionContainer"]`** — the eyebrow treatment. The app
   leans on `st.caption` for the explanatory notes above tables
   (`PERIOD_END_NOTE`, the mixed-source note on the horizon table). Mono,
   11px, uppercase, `0.12em` tracking is *wrong* for those: they are
   sentences, not labels. Set them in Inter Tight at 13px `--ink-faint`
   instead, and reserve the mono-uppercase style for a new short-label helper.
4. **`[data-testid="stTabs"] button`** — Streamlit's default underlined tab
   strip is close enough. Restyle the label to 14px/500 and let
   `primaryColor` colour the active underline.
5. **Container width** — lazyeconomist is `max-width: 1120px`. The app is
   `layout="wide"` because the horizon table is now up to fifteen columns.
   **Keep wide.** Do not import the 1120px measure; a data table constrained
   to a reading measure is worse on both counts.

## What does not transfer, and why

Being explicit here so nobody implements it and wonders why it feels wrong.

- **The hero.** `h1` at `clamp(44px, 7vw, 84px)` is a landing-page device. The
  app opens on a filter sidebar and a table. Cap h1 at 2.5rem.
- **The 1px-gap card grid.** It needs control over sibling layout that
  `st.columns` does not give without fighting the DOM. Use ordinary bordered
  containers.
- **100px section padding.** A marketing page has five sections and infinite
  scroll budget; a tab with a filter row, two metrics and a table does not.
  Halve it.
- **`backdrop-filter` sticky nav.** Streamlit owns the header. Leave it.
- **Dark mode.** lazyeconomist has no dark variant, and this palette is
  specifically a *paper* aesthetic that inverts badly — `#b8410e` on near-black
  is muddy. Set `base = "light"`; do not author `theme.dark.*`. Viewers whose
  OS prefers dark get the light theme, which is a deliberate choice, not an
  omission.

## Risks

- **Font licensing.** Fraunces (SIL OFL), Inter Tight (SIL OFL) and JetBrains
  Mono (SIL OFL) are all open-licence, so vendoring is permitted. Ship the
  `OFL.txt` alongside them.
- **`chartCategoricalColors` is one chart.** Low value; the palette above is a
  guess at a warm categorical ramp and should be checked against the actual
  allocation chart before being trusted.
- **`theme.sidebar.*` inherits nothing.** Heading sizes set under `[theme]` are
  explicitly *not* inherited by the sidebar. If the sidebar looks unstyled
  after the first pass, that is why, and it needs its own block.
- **Render restart.** `config.toml` is read at process start. The service must
  be restarted, not just redeployed to the same container.
- **Reviewing this is visual.** No test asserts a colour. The check is opening
  the app and looking at it, particularly the Performance tab's wide table and
  the Archive tab's long markdown.

## Plan

1. Vendor the three fonts into `static/fonts/`, add `OFL.txt`, set
   `server.enableStaticServing = true`.
2. Write `.streamlit/config.toml` with the `[theme]` block above and the
   `[[theme.fontFaces]]` entries.
3. Replace the CSS block in `app.py:49-72` with the four rules above.
4. Add a `_label()` helper for the mono-uppercase eyebrow, and use it where a
   short label is wanted — not on the explanatory captions.
5. Look at every tab. Fix the sidebar separately if it needs its own block.
6. Restart Render.

One test is worth writing despite the visual nature of the work:
`.streamlit/config.toml` parses and its `[theme]` keys are all real config
options — the failure mode for a typo'd key is that Streamlit ignores it
silently, which is exactly the class of bug `tests/test_deployment_config.py`
already exists to catch for `DATABASE_URL`.

---

## As built (2026-09-03)

The central claim held: **every value in `.streamlit/config.toml` validates
against `streamlit.config._config_options_template`, and no `.st-emotion-cache-*`
selector was needed.** The CSS that remains is four rules over two
`data-testid` selectors and three classes of our own.

### The trap the spec missed

It warned that `fontFaces` needs a `.woff2` rather than the `css2` stylesheet,
and that was right. It did **not** warn about the second silent failure in the
same step: **Google splits every family by `unicode-range`, and the largest
file is usually Cyrillic.** Picking by size — the obvious heuristic — ships a
font containing no Latin glyphs. Nothing errors; the browser falls back to the
system face and the page looks approximately styled.

Caught only because Inter Tight went from 89,800 to 44,872 bytes when the
selection changed to "the block whose `unicode-range` contains `U+0000`". The
first download had grabbed its Cyrillic subset. `static/fonts/LICENSES.md`
records the rule for whoever refreshes these next.

Vendored sizes: Fraunces 67,304 · Inter Tight 44,872 · JetBrains Mono 31,432 —
**143,608 bytes total**, all SIL OFL.

### Verified, not assumed

- All three fonts serve: `HTTP 200` at `app/static/fonts/*.woff2`, byte counts
  matching the files on disk.
- The theme applies — paper ground, Fraunces headings, burnt-orange active tab
  and links — confirmed by screenshot, not by the config parsing.
- `base = "light"` is doing real work: with the config removed the same build
  renders **dark**, following the OS preference.
- `tests/test_theme_config.py` — 9 tests, including that every declared font
  file exists and starts with `wOF2`, since a path typo also fails silently.

### Unrelated problem this surfaced

**The Performance and Plans tabs render nothing locally.** Reproduced on the
same build with the theme config removed, so it is not the restyle — it is
C2, the known local hang, and this is the first time it has been pinned to a
specific interaction: the app renders once, then a tab click never completes
its rerun. No error reaches the server log. That makes the visual review of
those two tabs incomplete; they were checked as far as the hang allows.

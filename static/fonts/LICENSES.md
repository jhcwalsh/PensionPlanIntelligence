# Vendored fonts

Three faces, self-hosted rather than loaded from Google's CDN so that a
rotated `fonts.gstatic.com` URL cannot silently change how the app renders,
and so the page makes no third-party request it does not need.

Each file is the **Latin subset** (`U+0000-00FF` and friends) of the variable
font, taken from the Google Fonts API. Google splits every family by
`unicode-range`, and the largest file is usually Cyrillic — shipping that one
gives a font with no Latin glyphs, which fails silently to the system
fallback. If these are ever refreshed, pick the block whose `unicode-range`
contains `U+0000`, not the biggest file.

| file | family | licence |
|---|---|---|
| `Fraunces.woff2` | Fraunces (variable, `opsz` 9–144, `wght` 300–700) | SIL Open Font License 1.1 |
| `InterTight.woff2` | Inter Tight (variable, `wght` 400–700) | SIL Open Font License 1.1 |
| `JetBrainsMono.woff2` | JetBrains Mono (variable, `wght` 400–600) | SIL Open Font License 1.1 |

All three are licensed under the **SIL Open Font License, Version 1.1**, which
permits embedding, redistribution and self-hosting provided the fonts are not
sold on their own and the licence travels with them.

- Fraunces — © The Fraunces Project Authors
  (https://github.com/undercasetype/Fraunces)
- Inter Tight — © The Inter Project Authors
  (https://github.com/rsms/inter)
- JetBrains Mono — © 2020 The JetBrains Mono Project Authors
  (https://github.com/JetBrains/JetBrainsMono)

Full licence text: https://openfontlicense.org/open-font-license-official-text/

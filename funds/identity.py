"""When are two fund names the same fund?

Pure functions, no I/O. A raw fund name ("Francisco Partners Fund VIII, L.P.")
parses to a series, a number and a vehicle; the identity key is
manager + series + number. Precision over recall throughout: the IC member
would rather miss a co-investor than be told about one that isn't there, so
anything ambiguous parses to something that will *not* collide.

- Different numbers are different funds; a vintage year counts as a number.
- Parallel vehicles ("III-A", "(Cayman)", "Parallel") roll up to the main
  fund, but the vehicle is kept so callers can show it.
- No manager, or neither series nor number, means no key at all.
"""
from __future__ import annotations

import re
from dataclasses import dataclass

_STOPWORDS = {"fund", "funds", "the", "lp", "llc", "scsp", "inc", "ltd"}

_MANAGER_SUFFIXES = {
    "inc", "llc", "lp", "llp", "ltd", "plc", "co", "corp", "corporation",
    "company", "group", "management", "advisors", "advisers", "sa", "ag", "gmbh",
}

# Words after the fund number that name a vehicle of the same fund.
_VEHICLE_AFTER = {
    "cayman", "parallel", "offshore", "onshore", "feeder", "delaware", "lux",
    "luxembourg", "us", "non-us", "aiv",
} | set("abcdefgh")

# Vehicle words that can also appear before the number ("Parallel Fund III").
_VEHICLE_BEFORE = {"parallel", "offshore", "cayman", "feeder"}

_ROMAN = {"i": 1, "v": 5, "x": 10, "l": 50}


@dataclass(frozen=True)
class ParsedFund:
    series: str
    number: int | None
    vehicle: str | None


def norm_tokens(s: str | None) -> list[str]:
    if not s:
        return []
    s = s.lower().replace("&", " and ")
    s = re.sub(r"[()\[\]/,;:]", " ", s)
    s = re.sub(r"[.'’\"]", "", s)          # "l.p." -> "lp", "o'neil" -> "oneil"
    return [t.strip("-") for t in s.split() if t.strip("-")]


def norm_manager(name: str | None) -> str:
    toks = norm_tokens(name)
    while len(toks) > 1 and toks[-1] in _MANAGER_SUFFIXES:
        toks.pop()
    return " ".join(toks)


def _int_to_roman(n: int) -> str:
    out = []
    for value, sym in ((50, "l"), (40, "xl"), (10, "x"), (9, "ix"),
                       (5, "v"), (4, "iv"), (1, "i")):
        while n >= value:
            out.append(sym)
            n -= value
    return "".join(out)


def roman_to_int(tok: str) -> int | None:
    tok = tok.lower()
    if not tok or any(c not in _ROMAN for c in tok):
        return None
    total = 0
    for i, c in enumerate(tok):
        v = _ROMAN[c]
        total += -v if i + 1 < len(tok) and _ROMAN[tok[i + 1]] > v else v
    if not 1 <= total <= 50 or _int_to_roman(total) != tok:
        return None
    return total


def _year(tok: str) -> int | None:
    return int(tok) if tok.isdigit() and 1990 <= int(tok) <= 2039 else None


def number_value(tok: str) -> int | None:
    """A fund number in any of its spellings: year, arabic 1-99, or roman."""
    if _year(tok) is not None:
        return int(tok)
    if tok.isdigit() and 1 <= int(tok) <= 99:
        return int(tok)
    return roman_to_int(tok)


def _strip_manager(toks: list[str], manager: str | None) -> list[str]:
    best = 0
    for cand in (norm_tokens(manager), norm_manager(manager).split()):
        k = 0
        while (k < len(cand) and k < len(toks) and toks[k] == cand[k]
               and number_value(toks[k]) is None):
            k += 1
        best = max(best, k)
    return toks[best:]


def _split_vehicle_suffix(toks: list[str]) -> list[str]:
    """'iii-a' -> ['iii', 'a'] when the left part is a fund number."""
    out = []
    for t in toks:
        m = re.fullmatch(r"(\w+)-([a-h])", t)
        if m and number_value(m.group(1)) is not None:
            out.extend([m.group(1), m.group(2)])
        else:
            out.append(t)
    return out


def parse_fund_name(raw: str, manager: str | None) -> ParsedFund:
    toks = _split_vehicle_suffix(norm_tokens(raw))
    toks = [t for t in toks if t not in _STOPWORDS]

    idx = next((i for i in range(len(toks) - 1, -1, -1)
                if number_value(toks[i]) is not None), None)
    if idx is None:
        return ParsedFund(" ".join(_strip_manager(toks, manager)), None, None)

    number = number_value(toks[idx])
    before, after = toks[:idx], toks[idx + 1:]
    vehicle: list[str] = []

    if idx == 0:
        # Leading vintage ("2024 Golden Bay"): what follows is the series.
        before, after = after, []

    if after and all(t in _VEHICLE_AFTER for t in after):
        vehicle, after = after, []

    before = _strip_manager(before, manager)
    vehicle = [t for t in before if t in _VEHICLE_BEFORE] + vehicle
    before = [t for t in before if t not in _VEHICLE_BEFORE]

    series = before + (["#"] + after if after else [])
    return ParsedFund(" ".join(series), number, " ".join(vehicle) or None)


def fund_key(manager_norm: str, parsed: ParsedFund) -> str | None:
    if not manager_norm or (not parsed.series and parsed.number is None):
        return None
    return f"{manager_norm}|{parsed.series}|{parsed.number if parsed.number is not None else ''}"

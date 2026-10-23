"""Fund-name parsing and identity keys (funds/identity.py).

These rules decide when two plans are reported as being in the same fund, and
a false match is worse than a miss, so each test pins one merge/no-merge call.
"""
from funds.identity import ParsedFund, fund_key, norm_manager, parse_fund_name, roman_to_int


def test_manager_prefix_and_stopwords_removed():
    assert parse_fund_name("GTCR Fund XV, L.P.", "GTCR") == ParsedFund("", 15, None)
    assert parse_fund_name("KKR North America Fund XIV", "KKR") == ParsedFund("north america", 14, None)


def test_spellings_of_same_fund_share_key():
    m = norm_manager("Francisco Partners")
    a = fund_key(m, parse_fund_name("Francisco Partners VIII", "Francisco Partners"))
    b = fund_key(m, parse_fund_name("Francisco Partners Fund VIII, L.P.", "Francisco Partners"))
    assert a == b


def test_agility_is_a_different_series():
    m = norm_manager("Francisco Partners")
    a = fund_key(m, parse_fund_name("Francisco Partners VIII", "Francisco Partners"))
    b = fund_key(m, parse_fund_name("Francisco Partners Agility IV", "Francisco Partners"))
    assert a != b


def test_different_numbers_differ():
    assert parse_fund_name("KKR North America Fund XIII", "KKR").number == 13


def test_vintage_year_is_number():
    assert parse_fund_name("2024 Golden Bay, L.P.", "Golden Bay").number == 2024


def test_parallel_vehicles_roll_up():
    p = parse_fund_name("Audax Direct Lending Solutions Fund III-A, L.P.", "Audax")
    assert (p.series, p.number, p.vehicle) == ("direct lending solutions", 3, "a")
    assert parse_fund_name("Advent GPE XI (Cayman)", "Advent").vehicle == "cayman"


def test_trailing_non_vehicle_words_stay_in_series():
    p = parse_fund_name("ASF IX Infrastructure B", "Ardian")
    assert p.number == 9 and "infrastructure" in p.series


def test_unnumbered_fund():
    assert parse_fund_name("Wellington Emerging Markets Fund", "Wellington") == ParsedFund("emerging markets", None, None)


def test_fund_key_none_without_manager():
    assert fund_key("", parse_fund_name("Fund III", None)) is None
    assert fund_key("acme", ParsedFund("", None, None)) is None


def test_manager_legal_suffix_stripped():
    assert norm_manager("BlackRock, Inc.") == norm_manager("BlackRock")
    assert norm_manager(None) == ""


def test_roman_bounds():
    assert roman_to_int("xiv") == 14
    assert roman_to_int("iiii") is None
    assert roman_to_int("mix") is None


def test_manager_stopwords_do_not_block_strip():
    assert parse_fund_name("Carlyle Partners VIII", "The Carlyle Group") == ParsedFund("partners", 8, None)


def test_generic_name_without_manager_words_has_no_key():
    m = norm_manager("Audax")
    assert fund_key(m, parse_fund_name("Fund III", "Audax")) is None
    assert fund_key(m, parse_fund_name("Audax Fund III", "Audax")) == "audax||3"

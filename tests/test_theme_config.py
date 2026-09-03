"""`.streamlit/config.toml` says what we think it says.

No test can assert that a page looks right, and none here tries. What they do
assert is the failure mode that looks like nothing: Streamlit **ignores an
unknown config key silently**, so `headingFontWeight` instead of
`headingFontWeights` is not an error, not a warning, and not a visible
difference from "the theme did not apply". Same reasoning as
tests/test_deployment_config.py for DATABASE_URL.

The fontFaces tests exist because that block has two traps that both fail
quietly: a stylesheet URL where a font file belongs, and a font file that does
not exist at the path given.
"""
import pathlib
import tomllib

import pytest

import streamlit.config as st_config

ROOT = pathlib.Path(__file__).resolve().parents[1]
CONFIG = ROOT / ".streamlit" / "config.toml"


@pytest.fixture(scope="module")
def cfg():
    assert CONFIG.exists(), f"{CONFIG} is missing"
    with open(CONFIG, "rb") as f:
        return tomllib.load(f)


def _flatten(d, prefix=""):
    """Config keys as dotted paths, treating arrays-of-tables as one key."""
    for k, v in d.items():
        key = f"{prefix}.{k}" if prefix else k
        if isinstance(v, dict):
            yield from _flatten(v, key)
        else:
            yield key


def test_every_key_is_a_real_streamlit_option(cfg):
    known = set(st_config._config_options_template)
    unknown = [k for k in _flatten(cfg) if k not in known]
    assert not unknown, (
        f"Streamlit ignores unknown config keys silently: {unknown}")


def test_the_palette_is_the_one_the_spec_names(cfg):
    theme = cfg["theme"]
    assert theme["backgroundColor"] == "#fbfaf7", "paper, not white"
    assert theme["primaryColor"] == "#b8410e"
    assert theme["borderColor"] == "#e8e4dc"
    assert theme["base"] == "light"


def test_no_dark_theme_is_declared(cfg):
    """Deliberate. This palette is a paper aesthetic and inverts badly --
    #b8410e on near-black is muddy. Light-only is a decision, not an omission,
    and a half-authored theme.dark would look like the omission."""
    assert "dark" not in cfg.get("theme", {})


def test_the_three_families_are_declared_and_used(cfg):
    faces = {f["family"] for f in cfg["theme"]["fontFaces"]}
    assert faces == {"Fraunces", "Inter Tight", "JetBrains Mono"}
    theme = cfg["theme"]
    assert theme["font"] == "Inter Tight"
    assert theme["headingFont"] == "Fraunces"
    assert theme["codeFont"] == "JetBrains Mono"


def test_font_urls_point_at_files_not_a_stylesheet(cfg):
    """The trap that costs an hour: theme.fontFaces takes a font file. Given
    a fonts.googleapis.com/css2 URL it registers nothing and says nothing."""
    for face in cfg["theme"]["fontFaces"]:
        url = face["url"]
        assert url.endswith(".woff2"), f"{face['family']}: {url}"
        assert "googleapis.com" not in url, (
            f"{face['family']} points at a stylesheet, not a font file")


def test_every_declared_font_file_exists_and_is_woff2(cfg):
    """A path typo also fails silently, falling back to the system stack."""
    for face in cfg["theme"]["fontFaces"]:
        # Streamlit serves static/ at the URL prefix "app/static/".
        rel = face["url"].removeprefix("app/")
        path = ROOT / rel
        assert path.exists(), f"{face['family']}: {path} missing"
        assert path.read_bytes()[:4] == b"wOF2", (
            f"{face['family']}: {path.name} is not a woff2 file")


def test_static_serving_is_enabled(cfg):
    """Without it the font files 404 and, again, nothing says so."""
    assert cfg["server"]["enableStaticServing"] is True


def test_heading_sizes_and_weights_are_six_long(cfg):
    """Streamlit fills unspecified entries with defaults, so a short list is
    not an error -- it just silently keeps some default sizes."""
    theme = cfg["theme"]
    assert len(theme["headingFontSizes"]) == 6
    assert len(theme["headingFontWeights"]) == 6


def test_h1_is_not_a_landing_page_hero(cfg):
    """The source clamps h1 up to 84px. This app opens on a filter sidebar and
    a fifteen-column table; the spec caps it deliberately."""
    h1 = cfg["theme"]["headingFontSizes"][0]
    assert h1.endswith("rem")
    assert float(h1.removesuffix("rem")) <= 2.75

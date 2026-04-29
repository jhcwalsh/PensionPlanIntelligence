"""
Stage-1 PDF-quality diagnostic.

Catches upstream extraction failures (blank pages, scanned-only pages,
garbled OCR text, irrelevant documents) so we can distinguish "the LLM
hallucinated" from "the PDF was unreadable".

Usage:
    from lib.pipeline_diagnostic import TASK_PROFILES, diagnose_document
    diag = diagnose_document(pdf_path, TASK_PROFILES["rfp"])
    if diag.verdict == "NO_TASK_CONTENT":
        ...  # skip extraction entirely
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Callable

# Verdict constants
STAGE_1_HEALTHY = "STAGE_1_HEALTHY"
STAGE_1_SUSPECTED = "STAGE_1_SUSPECTED"
NO_TASK_CONTENT = "NO_TASK_CONTENT"

# Page classification thresholds — tuned for board-packet PDFs in the
# existing corpus. Adjust if false-positive rates climb.
BLANK_MAX_CHARS = 50
GARBLED_NON_LETTER_RATIO = 0.40
GARBLED_MIN_CHARS = 200    # tiny pages aren't "garbled", they're just short


@dataclass(frozen=True)
class TaskProfile:
    """Per-task config for the diagnostic and the relevance filter."""
    name: str
    relevance_keywords: tuple[str, ...]
    min_relevant_pages: int = 1

    @property
    def relevance_regex(self) -> re.Pattern[str]:
        # Word boundaries on both sides; case-insensitive.
        joined = "|".join(re.escape(k) for k in self.relevance_keywords)
        return re.compile(rf"(?i)(?<![A-Za-z0-9])(?:{joined})(?![A-Za-z0-9])")


@dataclass
class DocumentDiagnosis:
    verdict: str
    blank_pages: int = 0
    scanned_pages: int = 0
    garbled_pages: int = 0
    task_relevant_pages: int = 0
    structure_score: float = 0.0
    rationale: list[str] = field(default_factory=list)
    page_count: int = 0


# Public registry — orchestrator and tests look up the RFP profile here.
TASK_PROFILES: dict[str, TaskProfile] = {
    "rfp": TaskProfile(
        name="rfp",
        relevance_keywords=(
            "RFP", "RFI", "RFQ",
            "Request for Proposal", "Request for Proposals",
            "Request for Information", "Request for Qualifications",
            "search committee", "search process",
            "finalists", "semi-finalists", "semifinalists",
            "shortlist", "short list",
            "incumbent", "issued", "response due", "responses received",
            "mandate", "search", "invitation to bid",
            "consultant search", "manager search",
            "selection", "candidate firms",
        ),
        min_relevant_pages=1,
    ),
}


def _classify_page(text: str, has_image: bool, profile: TaskProfile) -> dict[str, bool]:
    """Return classification flags for one page."""
    stripped = text.strip()
    char_count = len(stripped)

    is_blank = char_count < BLANK_MAX_CHARS and not has_image
    is_scanned = char_count < BLANK_MAX_CHARS and has_image

    is_garbled = False
    if char_count >= GARBLED_MIN_CHARS:
        non_letter = sum(1 for c in stripped if not (c.isalnum() or c.isspace() or c in ".,;:!?-'\""))
        ratio = non_letter / max(1, char_count)
        is_garbled = ratio > GARBLED_NON_LETTER_RATIO

    is_relevant = bool(profile.relevance_regex.search(stripped)) if char_count > 0 else False

    return {
        "blank": is_blank,
        "scanned": is_scanned,
        "garbled": is_garbled,
        "relevant": is_relevant,
    }


def _structure_score(page_count: int, blank: int, scanned: int, garbled: int) -> float:
    if page_count <= 0:
        return 0.0
    bad = blank + scanned + garbled
    return max(0.0, 1.0 - (bad / page_count))


def _open_with_pdfplumber(pdf_path: str) -> tuple[list[str], list[bool]]:
    """Per-page (text, has_image_flag). Heavy import deferred to call time."""
    import pdfplumber  # type: ignore[import-untyped]
    texts: list[str] = []
    has_images: list[bool] = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            texts.append(page.extract_text() or "")
            has_images.append(bool(page.images))
    return texts, has_images


def diagnose_document(
    pdf_path: str,
    profile: TaskProfile,
    loader: Callable[[], tuple[list[str], list[bool]]] | None = None,
) -> DocumentDiagnosis:
    """
    Inspect a PDF and return its Stage-1 verdict.

    Args:
        pdf_path: Local filesystem path to the PDF.
        profile: TaskProfile for the task (e.g., TASK_PROFILES["rfp"]).
        loader: Test/inject seam. If given, called instead of pdfplumber and
            must return (per_page_texts, per_page_has_image). Lets tests run
            without real PDFs and lets the orchestrator reuse the cached
            per-page text from documents.extracted_text.

    Returns:
        DocumentDiagnosis with verdict in {STAGE_1_HEALTHY, STAGE_1_SUSPECTED,
        NO_TASK_CONTENT} plus per-page counts and a rationale list.
    """
    if loader is not None:
        texts, has_images = loader()
    else:
        texts, has_images = _open_with_pdfplumber(pdf_path)

    page_count = len(texts)
    diag = DocumentDiagnosis(verdict=STAGE_1_HEALTHY, page_count=page_count)

    if page_count == 0:
        diag.verdict = NO_TASK_CONTENT
        diag.rationale.append("PDF has zero pages")
        return diag

    for text, has_image in zip(texts, has_images):
        flags = _classify_page(text, has_image, profile)
        if flags["blank"]:
            diag.blank_pages += 1
        if flags["scanned"]:
            diag.scanned_pages += 1
        if flags["garbled"]:
            diag.garbled_pages += 1
        if flags["relevant"]:
            diag.task_relevant_pages += 1

    diag.structure_score = _structure_score(
        page_count, diag.blank_pages, diag.scanned_pages, diag.garbled_pages
    )

    if diag.task_relevant_pages < profile.min_relevant_pages:
        diag.verdict = NO_TASK_CONTENT
        diag.rationale.append(
            f"Only {diag.task_relevant_pages} task-relevant page(s); "
            f"profile '{profile.name}' requires {profile.min_relevant_pages}"
        )
        return diag

    bad_pages = diag.blank_pages + diag.scanned_pages + diag.garbled_pages
    bad_ratio = bad_pages / page_count
    if bad_ratio > 0.30 or diag.scanned_pages >= 3 or diag.garbled_pages >= 2:
        diag.verdict = STAGE_1_SUSPECTED
        if diag.scanned_pages:
            diag.rationale.append(f"{diag.scanned_pages} scanned/image-only page(s) detected")
        if diag.garbled_pages:
            diag.rationale.append(f"{diag.garbled_pages} garbled page(s) detected")
        if bad_ratio > 0.30:
            diag.rationale.append(f"{bad_ratio:.0%} of pages have quality issues")
    else:
        diag.rationale.append(
            f"{diag.task_relevant_pages}/{page_count} pages relevant; "
            f"structure_score={diag.structure_score:.2f}"
        )

    return diag

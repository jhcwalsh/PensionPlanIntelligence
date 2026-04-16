"""Re-download failed ASRS documents using curl_cffi (Cloudflare-protected), then extract + summarize."""
import sys
from datetime import datetime
from pathlib import Path

if sys.platform == 'win32' and hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

from curl_cffi import requests as cr
from database import Document, get_session, init_db
from extractor import run_extractor
from summarizer import run_summarizer
from rich.console import Console

console = Console(legacy_windows=False)


def download_impersonated(url: str, dest_dir: Path, filename: str):
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / filename
    try:
        r = cr.get(url, impersonate="chrome124", timeout=60)
        if r.status_code != 200:
            return None, 0, f"HTTP {r.status_code}"
        with open(dest, "wb") as f:
            f.write(r.content)
        return dest, len(r.content), None
    except Exception as e:
        return None, 0, str(e)


def retry_failed(plan_id: str = "asrs"):
    init_db()
    session = get_session()
    redownloaded_ids = []
    try:
        docs = (
            session.query(Document)
            .filter(Document.plan_id == plan_id, Document.extraction_status == "failed")
            .all()
        )
        console.print(f"[bold]Retrying {len(docs)} failed downloads for {plan_id}[/bold]")

        dest_dir = Path("downloads") / plan_id

        for doc in docs:
            path, size, err = download_impersonated(doc.url, dest_dir, doc.filename)
            if path and size > 0:
                doc.local_path = str(path)
                doc.file_size_bytes = size
                doc.downloaded_at = datetime.utcnow()
                doc.extraction_status = "pending"
                redownloaded_ids.append(doc.id)
                console.print(f"  [green]OK[/green] {doc.filename} ({size:,} bytes)")
            else:
                console.print(f"  [red]FAIL[/red] {doc.filename} ({err})")

        session.commit()
        console.print(f"\n{len(redownloaded_ids)}/{len(docs)} re-downloaded")
    finally:
        session.close()

    if redownloaded_ids:
        console.rule("[bold]Extracting[/bold]")
        run_extractor(doc_ids=redownloaded_ids)
        console.rule("[bold]Summarizing[/bold]")
        run_summarizer(doc_ids=redownloaded_ids)


if __name__ == "__main__":
    retry_failed()

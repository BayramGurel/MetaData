#!/usr/bin/env python3
import warnings
import io
import json
import zipfile
import logging
import sys
import re
from pathlib import Path
from ckanapi import RemoteCKAN, NotFound
from tqdm import tqdm

# ─── CONFIG ────────────────────────────────────────────────────────────
CKAN_URL = "https://cautious-eureka-49499x555q7257v-5000.app.github.dev"
API_KEY  = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJqdGkiOiIwWFg2SE9YYXRIbnVpM1ZnaXNCNFdZOTlCdjk1ajJPWDhXcnUyRXdWbTVBIiwiaWF0IjoxNzQ4MzUwODcyfQ.flZiJl9NO32iFYLt0GZqGyVD6dNQufbssl2nMjQSjcY"
MANIFEST = Path(__file__).resolve().parent.parent / "report.json"
# ────────────────────────────────────────────────────────────────────────

warnings.filterwarnings("ignore", "pkg_resources is deprecated", UserWarning)
logging.basicConfig(level=logging.INFO, format="%(message)s")

def slugify(text: str) -> str:
    """Convert 'My File.pdf' → 'my-file' (valid CKAN slug)."""
    stem = Path(text).stem.lower()
    safe = re.sub(r"[^a-z0-9\-_]+", "-", stem)
    return safe.strip("-") or "resource"

def open_stream(spec: str) -> io.BytesIO:
    """
    Given a path like "outer.zip!/inner.zip!/file.ext",
    return a BytesIO of that entry.
    """
    parts = spec.replace("\\", "/").split("!/")
    data  = (MANIFEST.parent / parts[0].lstrip("./")).read_bytes()
    for entry in parts[1:]:
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            data = zf.read(entry)
    return io.BytesIO(data)

def ensure_dataset(ckan: RemoteCKAN, pkg_id: str, pkg_name: str) -> str:
    """Return an existing dataset ID or create one (slugified)."""
    try:
        if pkg_id:
            ckan.action.package_show(id=pkg_id)
            return pkg_id
        # else look up by slug(name) or create
        slug = slugify(pkg_name)
        pkg  = ckan.action.package_show(id=slug)
        return pkg["id"]
    except NotFound:
        created = ckan.action.package_create(name=slugify(pkg_name))
        logging.info("➤ created dataset %r → %s", pkg_name, created["id"])
        return created["id"]

def main():
    if not MANIFEST.exists():
        logging.error("Manifest not found at %s", MANIFEST)
        sys.exit(1)

    ckan = RemoteCKAN(CKAN_URL, apikey=API_KEY)
    try:
        ckan.action.status_show()
    except Exception as e:
        logging.error("Cannot reach CKAN at %s: %s", CKAN_URL, e)
        sys.exit(1)

    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    for res in tqdm(manifest.get("resources", []), desc="Uploading"):
        try:
            ds_id = ensure_dataset(
                ckan,
                res.get("package_id", ""),
                res.get("name", "")
            )
            stream = open_stream(res["upload"])
            fname  = res.get("extras", {})\
                       .get("original_filename", Path(res["upload"]).name)
            slug   = slugify(fname)

            result = ckan.action.resource_create(
                package_id    = ds_id,
                name          = slug,
                format        = res["format"],
                mimetype      = res["mimetype"],
                description   = res.get("description", ""),
                created       = res.get("created"),
                last_modified = res.get("last_modified"),
                extras        = json.dumps(res.get("extras", {}), ensure_ascii=False),
                upload        = (fname, stream),
            )

            logging.info("✔ %s → %s", fname, result["url"])

        except Exception as e:
            logging.error("✘ %s: %s", res["upload"], e)

if __name__ == "__main__":
    main()

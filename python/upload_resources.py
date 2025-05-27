#!/usr/bin/env python3
import warnings, io, json, zipfile, logging, sys, re
from pathlib import Path
from datetime import datetime
from ckanapi import RemoteCKAN, NotFound
from tqdm import tqdm

CKAN_URL = "https://…"
API_KEY   = "…"
MANIFEST  = Path(__file__).resolve().parent.parent / "report.json"

warnings.filterwarnings("ignore", "pkg_resources is deprecated", UserWarning)
logging.basicConfig(level=logging.INFO, format="%(message)s")

def slugify(text: str, maxlen=100) -> str:
    stem = Path(text).stem.lower()
    safe = re.sub(r"[^a-z0-9\-_]+", "-", stem).strip("-")
    return (safe or "resource")[:maxlen]

def open_stream(spec: str) -> io.BytesIO:
    parts = spec.replace("\\", "/").split("!/")
    data  = (MANIFEST.parent / parts[0].lstrip("./")).read_bytes()
    for entry in parts[1:]:
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            data = zf.read(entry)
    stream = io.BytesIO(data)
    stream.seek(0)
    return stream

def format_date(dt_str: str) -> str | None:
    if not dt_str:
        return None
    try:
        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%dT%H:%M:%S")
    except ValueError:
        logging.warning("Could not parse date %r", dt_str)
        return None

def ensure_dataset(ckan, pkg_id, pkg_name):
    try:
        if pkg_id:
            ckan.action.package_show(id=pkg_id)
            return pkg_id
        slug = slugify(pkg_name)
        pkg  = ckan.action.package_show(id=slug)
        return pkg["id"]
    except NotFound:
        created = ckan.action.package_create(name=slugify(pkg_name))
        logging.info("➤ created dataset %r → %s", pkg_name, created["id"])
        return created["id"]

def find_existing_resource(ckan, package_id, name):
    try:
        res_list = ckan.action.resource_search(package_id=package_id, query=name)
        for r in res_list.get("results", []):
            if r["name"] == name:
                return r["id"]
    except Exception:
        pass
    return None

def main():
    if not MANIFEST.exists():
        logging.error("Manifest not found at %s", MANIFEST)
        sys.exit(1)

    ckan = RemoteCKAN(CKAN_URL, apikey=API_KEY)
    try:
        ckan.action.status_show()
    except Exception as e:
        logging.error("Cannot reach CKAN: %s", e)
        sys.exit(1)

    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    for res in tqdm(manifest.get("resources", []), desc="Uploading"):
        try:
            ds_id = ensure_dataset(ckan, res.get("package_id", ""), res.get("name", ""))
            stream = open_stream(res["upload"])
            fname  = res.get("extras", {}).get("original_filename", Path(res["upload"]).name)
            slug   = slugify(fname)

            created_fmt      = format_date(res.get("created", ""))
            last_mod_fmt     = format_date(res.get("last_modified", ""))

            payload = {
                "package_id":    ds_id,
                "name":          slug,
                "format":        res["format"],
                "mimetype":      res["mimetype"],
                "description":   res.get("description", ""),
                "extras":        res.get("extras", {}),
                "license_id":    res.get("license_id", "cc-by"),
                "upload":        (fname, stream),
            }
            if created_fmt:
                payload["created"] = created_fmt
            if last_mod_fmt:
                payload["last_modified"] = last_mod_fmt

            # check for existing resource
            existing = find_existing_resource(ckan, ds_id, slug)
            if existing:
                payload["id"] = existing
                result = ckan.action.resource_update(**payload)
                logging.info("↻ %s (updated) → %s", fname, result["url"])
            else:
                result = ckan.action.resource_create(**payload)
                logging.info("✔ %s → %s", fname, result["url"])

        except Exception as e:
            logging.error("✘ %s: %s", res["upload"], e)

if __name__ == "__main__":
    main()

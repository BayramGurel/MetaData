import warnings
import io
import json
import zipfile
import logging
import sys
import re
from pathlib import Path
from datetime import datetime
from ckanapi import RemoteCKAN, NotFound
from tqdm import tqdm

# Configuration
CKAN_URL = "https://psychic-rotary-phone-94v44prrg9427gx6-5000.app.github.dev/"
API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJqdGkiOiJPNUlISmpYRWpxcTZNMXdGN1FYajFiMUU4WVVTNUFWTEFteXZBak1IMF9RIiwiaWF0IjoxNzUwMTU1ODE5fQ.3oUuqVytuQGj6RpN4nul6wMxmcDihpG47NF-H74PPY4"
MANIFEST = Path(__file__).resolve().parent.parent / "report.json"

warnings.filterwarnings("ignore", "pkg_resources is deprecated", UserWarning)
logging.basicConfig(level=logging.INFO, format="%(message)s")


def slugify(text: str, maxlen=100) -> str:
    stem = Path(text).stem.lower()
    safe = re.sub(r"[^a-z0-9\-_]+", "-", stem).strip("-")
    return (safe or "resource")[:maxlen]


def open_stream(spec: str) -> io.BytesIO:
    try:
        parts = spec.replace("\\", "/").split("!/")
        data = (MANIFEST.parent / parts[0].lstrip("./")).read_bytes()
        for entry in parts[1:]:
            with zipfile.ZipFile(io.BytesIO(data)) as zf:
                data = zf.read(entry)
        buf = io.BytesIO(data)
        buf.seek(0)
        return buf
    except Exception as e:
        logging.error(f"Error opening stream '{spec}': {e}")
        raise


def format_date(dt_str: str) -> str | None:
    if not dt_str:
        return None
    try:
        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%dT%H:%M:%S")
    except ValueError:
        logging.warning(f"Could not parse date: {dt_str}")
        return None


def ensure_dataset(ckan, meta: dict, org_id: str | None) -> str:
    title = meta.get("title", meta.get("name", Path(meta.get("upload", "")).stem))
    slug = slugify(title)
    description = meta.get("description", "")
    license_id = meta.get("license_id", "cc-BY")

    params = {"name": slug, "title": title, "notes": description, "license_id": license_id}
    if org_id:
        params["owner_org"] = org_id

    # DCAT-AP extras
    extras = {}
    for key in ("issued", "modified", "language", "spatial", "temporal", "publisher"):
        val = meta.get(key)
        if val:
            extras[f"dct_{key}"] = format_date(val) if key in ("issued", "modified") else val
    if extras:
        params["extras"] = json.dumps(extras)

    if "tags" in meta:
        params["tags"] = [{"name": t} for t in meta.get("tags", [])]
    if "groups" in meta:
        params["groups"] = [{"name": g} for g in meta.get("groups", [])]

    try:
        pkg = ckan.action.package_show(id=slug)
        params["id"] = pkg["id"]
        pkg = ckan.action.package_update(**params)
        logging.info(f"↻ Dataset updated: {pkg['id']}")
    except NotFound:
        pkg = ckan.action.package_create(**params)
        logging.info(f"✔ Dataset created: {pkg['id']}")
    return pkg["id"]


def find_existing_resource(ckan, package_id: str, name: str) -> str | None:
    try:
        for r in ckan.action.resource_search(package_id=package_id, query=name).get("results", []):
            if r["name"] == name:
                return r["id"]
    except Exception:
        pass
    return None


def delete_all_datasets(ckan):
    confirm = input("⚠️  Delete ALL datasets from CKAN? Type 'yes' to confirm: ")
    if confirm.lower() != "yes":
        logging.info("Deletion canceled.")
        return False
    for ds in ckan.action.package_list():
        try:
            ckan.action.package_delete(id=ds)
            logging.info(f"✖ Dataset deleted: {ds}")
        except Exception as e:
            logging.error(f"Could not delete dataset {ds}: {e}")
    return True


if __name__ == "__main__":
    if not MANIFEST.exists():
        logging.error(f"Manifest not found at {MANIFEST}")
        sys.exit(1)

    # Initialize CKAN client
    ckan = RemoteCKAN(CKAN_URL, apikey=API_KEY)
    try:
        ckan.action.status_show()
    except Exception as e:
        logging.error(f"CKAN not reachable: {e}")
        sys.exit(1)

    # Organization setup
    org_input = input("Enter organization name to assign datasets (leave blank for none): ").strip()
    if org_input:
        org_slug = slugify(org_input)
        try:
            ckan.action.organization_show(id=org_slug)
            org_id = org_slug
            logging.info(f"Using existing organization: {org_id}")
        except NotFound:
            org = ckan.action.organization_create(name=org_slug, title=org_input)
            org_id = org["id"]
            logging.info(f"✔ Organization created: {org_id}")
    else:
        org_id = None

    # Upload mode choice
    separate = input(
        "Create separate dataset per file? Type 'yes' for separate, anything else for single: "
    ).strip().lower() == 'yes'

    # Global delete option
    delete_all_datasets(ckan)

    # Load manifest
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))

    # Single dataset mode
    if not separate:
        ds_meta = manifest.get("dataset", {})
        ds_id = ensure_dataset(ckan, ds_meta, org_id)
        for res in tqdm(manifest.get("resources", []), desc="Uploading"):
            try:
                stream = open_stream(res["upload"])
                fname = res.get("extras", {}).get("original_filename", Path(res["upload"]).name)
                slug = slugify(fname)
                created = format_date(res.get("created", ""))
                modified = format_date(res.get("last_modified", ""))
                data = stream.read()
                solr_link = f"{CKAN_URL}dataset/{ds_id}/resource/{{id}}/preview"
                extras = {**res.get("extras", {})}
                extras.update({"view_url": solr_link})
                payload = {
                    "package_id": ds_id,
                    "name": slug,
                    "description": res.get("description", ""),
                    "format": res["format"],
                    "mimetype": res["mimetype"],
                    "upload": io.BytesIO(data),
                    **({"created": created} if created else {}),
                    **({"last_modified": modified} if modified else {}),
                    "extras": json.dumps(extras)
                }
                existing = find_existing_resource(ckan, ds_id, slug)
                if existing:
                    payload["id"] = existing
                    result = ckan.action.resource_update(**payload)
                    logging.info(f"↻ Updated: {fname} → {result['url']}")
                else:
                    result = ckan.action.resource_create(**payload)
                    logging.info(f"✔ Added: {fname} → {result['url']}")
            except Exception as e:
                logging.exception(f"✘ Error processing {res.get('upload','unknown')}: {e}")

    # Separate dataset mode
    else:
        for res in tqdm(manifest.get("resources", []), desc="Processing files"):
            try:
                file_meta = {
                    "title": res.get("extras", {}).get("original_filename", Path(res["upload"]).name),
                    "description": res.get("description", ""),
                    "license_id": manifest.get("dataset", {}).get("license_id", "cc-BY")
                }
                ds_id = ensure_dataset(ckan, file_meta, org_id)
                stream = open_stream(res["upload"])
                fname = file_meta["title"]
                slug = slugify(fname)
                created = format_date(res.get("created", ""))
                modified = format_date(res.get("last_modified", ""))
                data = stream.read()
                solr_link = f"{CKAN_URL}dataset/{ds_id}/resource/{{id}}/preview"
                extras = {**res.get("extras", {})}
                extras.update({"view_url": solr_link})
                payload = {
                    "package_id": ds_id,
                    "name": slug,
                    "description": res.get("description", ""),
                    "format": res["format"],
                    "mimetype": res["mimetype"],
                    "upload": io.BytesIO(data),
                    **({"created": created} if created else {}),
                    **({"last_modified": modified} if modified else {}),
                    "extras": json.dumps(extras)
                }
                existing = find_existing_resource(ckan, ds_id, slug)
                if existing:
                    payload["id"] = existing
                    result = ckan.action.resource_update(**payload)
                    logging.info(f"↻ Updated resource in {ds_id}: {fname} → {result['url']}")
                else:
                    result = ckan.action.resource_create(**payload)
                    logging.info(f"✔ Created resource in {ds_id}: {fname} → {result['url']}")
            except Exception as e:
                logging.exception(f"✘ Error processing {res.get('upload','unknown')}: {e}")

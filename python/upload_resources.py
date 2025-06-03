import warnings, io, json, zipfile, logging, sys, re
from pathlib import Path
from datetime import datetime
from ckanapi import RemoteCKAN, NotFound
from tqdm import tqdm

CKAN_URL = "https://musical-goldfish-94v44prr4r73xqwj-5000.app.github.dev/"
API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJqdGkiOiJZSVdpRGtSTTlCd1ZGTHZJZmJHYThSR1VVNnZsWXlxUFhHbXpFdXp6NXZZIiwiaWF0IjoxNzQ4OTU3MzI5fQ.5-oUXQYgyiG8hp482HlcAr2R-MLRjOAoQqvBVnqO-sc"
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
                if entry not in zf.namelist():
                    raise FileNotFoundError(f"File '{entry}' not found in ZIP.")
                data = zf.read(entry)
        stream = io.BytesIO(data)
        stream.seek(0)
        return stream
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
        logging.warning("Could not parse date: %r", dt_str)
        return None

def ensure_dataset(ckan, name):
    try:
        return ckan.action.package_show(id=name)["id"]
    except NotFound:
        created = ckan.action.package_create(name=name, title=name.replace("-", " ").title())
        logging.info("➤ Created dataset: '%s' → %s", name, created["id"])
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

def delete_all_datasets(ckan):
    confirm = input("⚠️  Delete ALL datasets in CKAN? Type 'yes' to confirm: ")
    if confirm.lower() != "yes":
        logging.info("Deletion cancelled.")
        return
    datasets = ckan.action.package_list()
    for ds in datasets:
        try:
            ckan.action.package_delete(id=ds)
            logging.info("✖ Deleted dataset: %s", ds)
        except Exception as e:
            logging.error("Could not delete dataset %s: %s", ds, e)

def main():
    if not MANIFEST.exists():
        logging.error("Manifest not found at %s", MANIFEST)
        sys.exit(1)

    ckan = RemoteCKAN(CKAN_URL, apikey=API_KEY)
    try:
        ckan.action.status_show()
    except Exception as e:
        logging.error("CKAN not reachable: %s", e)
        sys.exit(1)

    # Enable full wipe (be careful!)
    # delete_all_datasets(ckan)

    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    resources = manifest.get("resources", [])

    if not resources:
        logging.error("No resources found in manifest.")
        sys.exit(1)

    dataset_name = resources[0].get("package_id", "default-dataset")
    dataset_id = ensure_dataset(ckan, dataset_name)

    for res in tqdm(resources, desc=f"Uploading to {dataset_name}"):
        try:
            stream = open_stream(res["upload"])
            fname = res.get("extras", {}).get("original_filename", Path(res["upload"]).name)
            slug = slugify(fname)
            created_fmt = format_date(res.get("created", ""))
            modified_fmt = format_date(res.get("last_modified", ""))

            payload = {
                "package_id": dataset_id,
                "name": slug,
                "format": res["format"],
                "mimetype": res["mimetype"],
                "description": res.get("description", ""),
                "license_id": res.get("license_id", "cc-by"),
                "upload": stream
            }

            if created_fmt:
                payload["created"] = created_fmt
            if modified_fmt:
                payload["last_modified"] = modified_fmt

            extras_dict = res.get("extras", {})
            if isinstance(extras_dict, dict) and extras_dict:
                payload["extras"] = json.dumps(extras_dict)

            existing = find_existing_resource(ckan, dataset_id, slug)
            if existing:
                payload["id"] = existing
                result = ckan.action.resource_update(**payload)
                logging.info("↻ Updated: %s → %s", fname, result["url"])
            else:
                result = ckan.action.resource_create(**payload)
                logging.info("✔ Uploaded: %s → %s", fname, result["url"])

        except Exception as e:
            logging.exception("✘ Error processing %s: %s", res.get("upload", "unknown"), e)

if __name__ == "__main__":
    main()

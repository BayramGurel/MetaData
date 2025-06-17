import warnings, io, json, zipfile, logging, sys, re
from pathlib import Path
from datetime import datetime
from ckanapi import RemoteCKAN, NotFound
from tqdm import tqdm

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
                if entry not in zf.namelist():
                    raise FileNotFoundError(f"Bestand '{entry}' niet gevonden in ZIP.")
                data = zf.read(entry)
        stream = io.BytesIO(data)
        stream.seek(0)
        return stream
    except Exception as e:
        logging.error(f"Fout bij openen van stream '{spec}': {e}")
        raise

def format_date(dt_str: str) -> str | None:
    if not dt_str:
        return None
    try:
        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%dT%H:%M:%S")
    except ValueError:
        logging.warning("Kon datum niet parsen: %r", dt_str)
        return None

def ensure_dataset(ckan, pkg_id, pkg_name):
    try:
        if pkg_id:
            return ckan.action.package_show(id=pkg_id)["id"]
        slug = slugify(pkg_name)
        return ckan.action.package_show(id=slug)["id"]
    except NotFound:
        created = ckan.action.package_create(name=slugify(pkg_name), title=pkg_name)
        logging.info("➤ Dataset aangemaakt: '%s' → %s", pkg_name, created["id"])
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
    confirm = input("⚠️  Alles verwijderen uit CKAN? Type 'yes' om door te gaan: ")
    if confirm.lower() != "yes":
        logging.info("Verwijderen geannuleerd.")
        return
    datasets = ckan.action.package_list()
    for ds in datasets:
        try:
            ckan.action.package_delete(id=ds)
            logging.info("✖ Dataset verwijderd: %s", ds)
        except Exception as e:
            logging.error("Kon dataset %s niet verwijderen: %s", ds, e)

def main():
    if not MANIFEST.exists():
        logging.error("Manifest niet gevonden op %s", MANIFEST)
        sys.exit(1)

    ckan = RemoteCKAN(CKAN_URL, apikey=API_KEY)
    try:
        ckan.action.status_show()
    except Exception as e:
        logging.error("CKAN niet bereikbaar: %s", e)
        sys.exit(1)

    delete_all_datasets(ckan)  # → alleen aanzetten als je zeker bent

    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    for res in tqdm(manifest.get("resources", []), desc="Uploaden"):
        try:
            ds_id = ensure_dataset(ckan, res.get("package_id", ""), res.get("name", ""))
            stream = open_stream(res["upload"])
            fname = res.get("extras", {}).get("original_filename", Path(res["upload"]).name)
            slug = slugify(fname)
            created_fmt = format_date(res.get("created", ""))
            modified_fmt = format_date(res.get("last_modified", ""))

            payload = {
                "package_id": ds_id,
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

            existing = find_existing_resource(ckan, ds_id, slug)
            if existing:
                payload["id"] = existing
                result = ckan.action.resource_update(**payload)
                logging.info("↻ Bijgewerkt: %s → %s", fname, result["url"])
            else:
                result = ckan.action.resource_create(**payload)
                logging.info("✔ Toegevoegd: %s → %s", fname, result["url"])

        except Exception as e:
            logging.exception("✘ Fout bij verwerken van %s: %s", res.get("upload", "onbekend"), e)

if __name__ == "__main__":
    main()
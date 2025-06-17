import warnings, io, json, zipfile, logging, sys, re, hashlib
from pathlib import Path
from datetime import datetime
from ckanapi import RemoteCKAN, NotFound
from tqdm import tqdm

# Configuratie
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
        logging.error(f"Fout bij openen van stream '{spec}': {e}")
        raise

def format_date(dt_str: str) -> str | None:
    if not dt_str:
        return None
    try:
        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%dT%H:%M:%S")
    except ValueError:
        logging.warning(f"Kon datum niet parsen: {dt_str}")
        return None

def ensure_dataset(ckan, meta: dict) -> str:
    title       = meta.get("title", meta.get("name", "[Provincie zuid-holland]"))
    slug        = slugify(title)
    description = meta.get("description", "")
    license_id  = meta.get("license_id", "cc-BY")

    # DCAT-AP extras
    extras = {
        k: v for k, v in {
            "dct_issued":   format_date(meta.get("issued", "")),
            "dct_modified": format_date(meta.get("modified", "")),
            "dct_language": meta.get("language", ""),
            "dct_spatial":  meta.get("spatial", ""),
            "dct_temporal": meta.get("temporal", ""),
            "dct_publisher": meta.get("publisher", "")
        }.items() if v
    }
    tags   = meta.get("tags", [])
    groups = meta.get("groups", [])
    owner  = meta.get("organization", {}).get("id", None)

    params = {"name": slug, "title": title, "notes": description, "license_id": license_id}
    if extras:
        params["extras"] = json.dumps(extras)
    if tags:
        params["tags"] = [{"name": t} for t in tags]
    if groups:
        params["groups"] = [{"name": g} for g in groups]
    if owner:
        params["owner_org"] = owner

    try:
        pkg = ckan.action.package_show(id=slug)
        params["id"] = pkg["id"]
        pkg = ckan.action.package_update(**params)
        logging.info(f"↻ Dataset bijgewerkt: {pkg['id']}")
    except NotFound:
        pkg = ckan.action.package_create(**params)
        logging.info(f"✔ Dataset aangemaakt: {pkg['id']}")
    return pkg["id"]

def find_existing_resource(ckan, package_id: str, name: str) -> str | None:
    try:
        for r in ckan.action.resource_search(package_id=package_id, query=name).get("results", []):
            if r["name"] == name:
                return r["id"]
    except:
        pass
    return None

def delete_all_datasets(ckan):
    confirm = input("⚠️  Alles verwijderen uit CKAN? Type 'yes' om door te gaan: ")
    if confirm.lower() != "yes":
        logging.info("Verwijderen geannuleerd.")
        return
    for ds in ckan.action.package_list():
        try:
            ckan.action.package_delete(id=ds)
            logging.info(f"✖ Dataset verwijderd: {ds}")
        except Exception as e:
            logging.error(f"Kon dataset {ds} niet verwijderen: {e}")

if __name__ == "__main__":
    if not MANIFEST.exists():
        logging.error(f"Manifest niet gevonden op {MANIFEST}")
        sys.exit(1)

    ckan = RemoteCKAN(CKAN_URL, apikey=API_KEY)
    try:
        ckan.action.status_show()
    except Exception as e:
        logging.error(f"CKAN niet bereikbaar: {e}")
        sys.exit(1)

    delete_all_datasets(ckan)  # alleen inschakelen als je écht wilt resetten

    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    # dataset-metadata uit manifest als aanwezig
    ds_meta = manifest.get("dataset", {})
    ds_id   = ensure_dataset(ckan, ds_meta)

    for res in tqdm(manifest.get("resources", []), desc="Uploaden"):
        try:
            # bronbestand openen
            stream = open_stream(res["upload"])
            fname = res.get("extras", {}).get("original_filename", Path(res["upload"]).name)
            slug  = slugify(fname)

            created  = format_date(res.get("created", ""))
            modified = format_date(res.get("last_modified", ""))

            # lees data in geheugen
            data = stream.read()
            # Solr/view-link
            solr_link = f"{CKAN_URL}dataset/{ds_id}/resource/{{id}}/preview"

            # extras voor resource (inclusief view_url)
            extras = {**res.get("extras", {})}
            extras.update({"view_url": solr_link})

            payload = {
                "package_id":   ds_id,
                "name":         slug,
                "description":  res.get("description", ""),
                "format":       res["format"],
                "mimetype":     res["mimetype"],
                "upload":       io.BytesIO(data),
                **({"created": created}   if created else {}),
                **({"last_modified": modified} if modified else {}),
                "extras":       json.dumps(extras)
            }

            existing = find_existing_resource(ckan, ds_id, slug)
            if existing:
                payload["id"] = existing
                result = ckan.action.resource_update(**payload)
                logging.info(f"↻ Bijgewerkt: {fname} → {result['url']}")
            else:
                result = ckan.action.resource_create(**payload)
                logging.info(f"✔ Toegevoegd : {fname} → {result['url']}")

        except Exception as e:
            logging.exception(f"✘ Fout bij verwerken van {res.get('upload','onbekend')}: {e}")

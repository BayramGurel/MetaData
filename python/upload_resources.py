#!/usr/bin/env python3
import warnings
# suppress that pkg_resources warning from ckanapi
warnings.filterwarnings("ignore", "pkg_resources is deprecated", UserWarning)

#!/usr/bin/env python3
import io, json, zipfile, logging
from pathlib import Path
from ckanapi import RemoteCKAN
from tqdm import tqdm

# ─── CONFIG ─────────────────────────────────────────────────────────
CKAN_URL = "https://your-real-ckan.example.com"
API_KEY  = "abcdef12-3456-7890-abcd-ef1234567890"
MANIFEST = Path.home() / "Documents/GitHub/MetaData/report.json"
# ────────────────────────────────────────────────────────────────────

def open_stream(spec):
    parts = spec.replace("\\", "/").split("!/")
    data = (MANIFEST.parent / parts[0].lstrip("./")).read_bytes()
    for p in parts[1:]:
        data = zipfile.ZipFile(io.BytesIO(data)).read(p)
    return io.BytesIO(data)

def main():
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    ckan = RemoteCKAN(CKAN_URL, apikey=API_KEY)
    manifest = json.loads(MANIFEST.read_text())
    for res in tqdm(manifest["resources"], desc="Uploading"):
        try:
            stream = open_stream(res["upload"])
            name   = Path(res["upload"]).name
            fname  = res.get("extras", {}).get("original_filename", name)
            ckan.action.resource_create(
                package_id    = res["package_id"],
                name          = res["name"],
                format        = res["format"],
                mimetype      = res["mimetype"],
                description   = res.get("description", ""),
                created       = res.get("created"),
                last_modified = res.get("last_modified"),
                extras        = json.dumps(res.get("extras", {}), ensure_ascii=False),
                upload        = (fname, stream),
            )
            logging.info("✔ %s", fname)
        except Exception as e:
            logging.error("✘ %s: %s", res["upload"], e)

if __name__ == "__main__":
    main()

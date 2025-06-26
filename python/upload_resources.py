#!/usr/bin/env python3
"""
Refactored CKAN Uploader Script
Maintains interactive prompts and behavior, with improved structure, type hints, and logging.
Enhanced error message for CKAN connection failures.
"""
import warnings
import io
import json
import zipfile
import logging
import re
import hashlib
import sys
import subprocess
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any, List, Union

from ckanapi import RemoteCKAN, NotFound, ValidationError
from tqdm import tqdm

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
CKAN_URL: str = "https://special-space-disco-94v44prrppr36q6-5000.app.github.dev/"
API_KEY: str = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJqdGkiOiJmZDRjUEVrQkVRaVRNNXJ3VUMtYV4N0R1YlhzeTlZMmlrZlpVa0VRRVJnI"
    "iwiaWF0IjoxNzUwMzM3MDQ1fQ.kXvgCvs7Emc7RfPxGZ1znLz7itMqK4p0hXYoEoc8LaA"
)
MANIFEST: Path = Path(__file__).resolve().parent.parent / "report.json"
EXTRACTOR: Path = Path(__file__).resolve().parent.parent / "src" / "MetadataExtractor.java"

warnings.filterwarnings("ignore", "pkg_resources is deprecated", UserWarning)
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Utility Functions
# -----------------------------------------------------------------------------
def slugify(text: str, maxlen: int = 100) -> str:
    """Generate URL-safe slug from text."""
    stem = Path(text).stem.lower()
    slug = re.sub(r'[^a-z0-9\-_]+', '-', stem).strip('-') or 'resource'
    if len(slug) > maxlen:
        suffix = hashlib.sha1(slug.encode()).hexdigest()[:6]
        slug = f"{slug[:maxlen-7].rstrip('-')}-{suffix}"
    return slug


def format_date(dt_str: Union[str, None]) -> Optional[str]:
    """Convert ISO8601 timestamp to CKAN-compatible string."""
    if not dt_str:
        return None
    try:
        dt = datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
        return dt.strftime('%Y-%m-%dT%H:%M:%S')
    except ValueError:
        logger.warning("Could not parse date '%s', skipping.", dt_str)
        return None


def open_stream(spec: str, base_dir: Path) -> io.BytesIO:
    """Open a file or nested zip entry as a byte stream."""
    parts = spec.replace('\\', '/').split('!/')
    data = (base_dir / parts[0].lstrip('./')).read_bytes()
    for entry in parts[1:]:
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            data = zf.read(entry)
    buf = io.BytesIO(data)
    buf.seek(0)
    return buf


def safe_create_package(ckan: RemoteCKAN, params: Dict[str, Any]) -> Dict[str, Any]:
    """Create a CKAN package, purging trashed slug on conflict."""
    try:
        return ckan.action.package_create(**params)
    except ValidationError as exc:
        errors = getattr(exc, 'error_dict', {}) or {}
        name_errs = errors.get('name', [])
        if any('already exists' in msg for msg in name_errs):
            slug = params['name']
            logger.warning("Slug '%s' trashed—purging and retrying...", slug)
            try:
                ckan.action.dataset_purge(id=slug)
            except Exception:
                pass
            return ckan.action.package_create(**params)
        raise

# -----------------------------------------------------------------------------
# Deletion Helpers
# -----------------------------------------------------------------------------
def delete_all_datasets(ckan: RemoteCKAN) -> None:
    """Delete and purge all datasets."""
    for slug in ckan.action.package_list():
        logger.info("Deleting dataset: %s", slug)
        try:
            ckan.action.package_delete(id=slug)
            ckan.action.dataset_purge(id=slug)
            logger.info("  Purged: %s", slug)
        except Exception as e:
            logger.error("  Failed to delete %s: %s", slug, e)


def delete_all_organizations(ckan: RemoteCKAN) -> None:
    """Delete and purge all organizations (and their datasets)."""
    for slug in ckan.action.organization_list():
        logger.info("Deleting organization: %s", slug)
        delete_all_datasets(ckan)
        try:
            ckan.action.organization_delete(id=slug)
            ckan.action.organization_purge(id=slug)
            logger.info("  Purged org: %s", slug)
        except Exception as e:
            logger.error("  Failed to delete org %s: %s", slug, e)

# -----------------------------------------------------------------------------
# CKAN Operations
# -----------------------------------------------------------------------------
def ensure_dataset(ckan: RemoteCKAN, meta: Dict[str, Any], org_id: Optional[str]) -> str:
    """Create or update a dataset, returning its ID."""
    title = meta.get('title') or Path(meta.get('upload', '')).stem
    slug = slugify(title)
    params: Dict[str, Any] = {
        'name': slug,
        'title': title,
        'notes': meta.get('description', ''),
        'license_id': meta.get('license_id', 'cc-BY'),
    }
    if org_id:
        params['owner_org'] = org_id

    extras: Dict[str, Any] = {}
    for key in ('issued', 'modified', 'language', 'spatial', 'temporal', 'publisher'):
        if value := meta.get(key):
            extras[f'dct_{key}'] = format_date(value) if key in ('issued', 'modified') else value
    if extras:
        params['extras'] = json.dumps(extras)
    if tags := meta.get('tags'):
        params['tags'] = [{'name': t} for t in tags]
    if groups := meta.get('groups'):
        params['groups'] = [{'name': g} for g in groups]

    try:
        pkg = ckan.action.package_show(id=slug)
        if pkg.get('state') == 'deleted':
            logger.info("Found trashed '%s', purging...", slug)
            try:
                ckan.action.dataset_purge(id=slug)
            except Exception:
                pass
            pkg = safe_create_package(ckan, params)
            logger.info("Re-created dataset: %s", pkg['id'])
            return pkg['id']
        params['id'] = pkg['id']
        updated = ckan.action.package_update(**params)
        logger.info("Updated dataset: %s", updated['id'])
        return updated['id']
    except NotFound:
        created = safe_create_package(ckan, params)
        logger.info("Created dataset: %s", created['id'])
        return created['id']


def find_existing_resource(ckan: RemoteCKAN, pkg_id: str, name: str) -> Optional[str]:
    """Return an existing resource ID by name, if it exists."""
    try:
        res = ckan.action.resource_search(filters={'package_id': pkg_id, 'name': name})
        return res['results'][0]['id'] if res.get('results') else None
    except Exception:
        return None

# -----------------------------------------------------------------------------
# CKAN Connection Helper
# -----------------------------------------------------------------------------
def connect_ckan() -> RemoteCKAN:
    """Establish a connection to CKAN, exiting early on failure."""
    try:
        ckan = RemoteCKAN(CKAN_URL, apikey=API_KEY)
        ckan.action.status_show()
        return ckan
    except Exception as e:
        logger.error(
            "ERROR: Cannot connect to CKAN at '%s': %s\n"
            "Possible causes: CKAN server not running, incorrect URL or API endpoint, network/firewall restrictions, or invalid API key.",
            CKAN_URL,
            e,
        )
        sys.exit(1)

# -----------------------------------------------------------------------------
# Main Workflow
# -----------------------------------------------------------------------------
def main() -> None:
    # Verify manifest early
    if not MANIFEST.exists():
        logger.error("ERROR: Manifest not found at %s", MANIFEST)
        sys.exit(1)
    base_dir = MANIFEST.parent

    # Connect to CKAN before any interactive prompts
    ckan = connect_ckan()

    # Optionally run Java metadata extractor
    if input("Run MetadataExtractor? Type 'yes' to confirm: ").strip().lower() == 'yes':
        logger.info("Executing extractor: %s", EXTRACTOR)
        subprocess.run(
            ["java", str(EXTRACTOR), "--output", str(MANIFEST)],
            check=True,
            cwd=EXTRACTOR.parent,
        )

    # Interactive configuration prompts
    print("\n=== CONFIGURATION ===")
    wipe_orgs = (input("Delete ALL organizations? Type 'yes' to confirm: ").strip().lower() == 'yes')
    wipe_ds = (input("Delete ALL datasets?      Type 'yes' to confirm: ").strip().lower() == 'yes')
    org_input = input("Enter new organization name (leave blank for none): ").strip() or None
    separate = (input("Separate dataset per file? Type 'yes' for separate: ").strip().lower() == 'yes')
    print("======================\n")

    # Perform deletions
    if wipe_orgs:
        print(">> Deleting all organizations...")
        delete_all_organizations(ckan)
        print()
    if wipe_ds:
        print(">> Deleting all datasets...")
        delete_all_datasets(ckan)
        print()

    # Organization creation/use
    org_id: Optional[str] = None
    if org_input:
        slug = slugify(org_input)
        try:
            ckan.action.organization_show(id=slug)
            org_id = slug
            print(f">> Using existing org: {org_id}")
        except NotFound:
            org = ckan.action.organization_create(name=slug, title=org_input)
            org_id = org['id']
            print(f">> Created new org:   {org_id}")
        print()

    # Load manifest and prepare upload
    manifest_data = json.loads(MANIFEST.read_text(encoding='utf-8'))
    resources: List[Dict[str, Any]] = manifest_data.get('resources', [])
    ds_meta: Dict[str, Any] = manifest_data.get('dataset', {})

    mode = "per-file datasets" if separate else "single dataset"
    print(f">> Uploading in {mode} mode\n")

    # Upload resources
    if not separate:
        ds_id = ensure_dataset(ckan, ds_meta, org_id)
        print(f"→ Using dataset: {ds_id}\n")
        for res in tqdm(resources, desc="Uploading"):
            stream = open_stream(res['upload'], base_dir)
            fname = res.get('extras', {}).get('original_filename', Path(res['upload']).name)
            rslug = slugify(fname)
            created = format_date(res.get('created'))
            modified = format_date(res.get('last_modified'))
            data = stream.read()
            solr_url = f"{CKAN_URL}dataset/{ds_id}/resource/{{id}}/preview"
            extras = {**res.get('extras', {}), 'view_url': solr_url}

            payload: Dict[str, Any] = {
                'package_id': ds_id,
                'name': rslug,
                'description': res.get('description', ''),
                'format': res['format'],
                'mimetype': res['mimetype'],
                'upload': io.BytesIO(data),
                'extras': json.dumps(extras),
            }
            if created:
                payload['created'] = created
            if modified:
                payload['last_modified'] = modified

            if existing := find_existing_resource(ckan, ds_id, rslug):
                payload['id'] = existing
                out = ckan.action.resource_update(**payload)
                logger.info("↻ Updated: %s → %s", fname, out['url'])
            else:
                out = ckan.action.resource_create(**payload)
                logger.info("✔ Added:   %s → %s", fname, out['url'])
    else:
        for res in tqdm(resources, desc="Per-file datasets"):
            title = res.get('extras', {}).get('original_filename', Path(res['upload']).name)
            file_meta = {
                'title': title,
                'description': res.get('description', ''),
                'license_id': ds_meta.get('license_id', 'cc-BY'),
                'upload': res['upload'],
            }
            ds_id = ensure_dataset(ckan, file_meta, org_id)
            print(f"\n→ Dataset: {ds_id} ({title})")

            stream = open_stream(res['upload'], base_dir)
            rslug = slugify(title)
            created = format_date(res.get('created'))
            modified = format_date(res.get('last_modified'))
            data = stream.read()
            solr_url = f"{CKAN_URL}dataset/{ds_id}/resource/{{id}}/preview"
            extras = {**res.get('extras', {}), 'view_url': solr_url}

            payload = {
                'package_id': ds_id,
                'name': rslug,
                'description': res.get('description', ''),
                'format': res['format'],
                'mimetype': res['mimetype'],
                'upload': io.BytesIO(data),
                'extras': json.dumps(extras),
            }
            if created:
                payload['created'] = created
            if modified:
                payload['last_modified'] = modified

            if existing := find_existing_resource(ckan, ds_id, rslug):
                payload['id'] = existing
                out = ckan.action.resource_update(**payload)
                logger.info("↻ Updated: %s → %s", title, out['url'])
            else:
                out = ckan.action.resource_create(**payload)
                logger.info("✔ Created: %s → %s", title, out['url'])

if __name__ == '__main__':
    main()

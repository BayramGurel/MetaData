#!/usr/bin/env python3
import warnings
import io
import json
import zipfile
import logging
import re
import hashlib
import sys
from pathlib import Path
from datetime import datetime

from ckanapi import RemoteCKAN, NotFound, ValidationError
from tqdm import tqdm

# -----------------------------------------------------------------------------
# Configuration (hard-coded)
# -----------------------------------------------------------------------------
CKAN_URL = "https://special-space-disco-94v44prrppr36q6-5000.app.github.dev/"
API_KEY  = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJqdGkiOiJmZDRjUEVrQkVRaVRNNXJ3VUMtYVY4N0R1YlhzeTlZMmlrZlpVa0VRRVJnIiwiaWF0IjoxNzUwMzM3MDQ1fQ.kXvgCvs7Emc7RfPxGZ1znLz7itMqK4p0hXYoEoc8LaA"
MANIFEST = Path(__file__).resolve().parent.parent / "report.json"

# -----------------------------------------------------------------------------
# Silence deprecation warnings
# -----------------------------------------------------------------------------
warnings.filterwarnings("ignore", "pkg_resources is deprecated", UserWarning)

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Utility functions
# -----------------------------------------------------------------------------
def slugify(text: str, maxlen: int = 100) -> str:
    stem = Path(text).stem.lower()
    safe = re.sub(r'[^a-z0-9\-_]+', '-', stem).strip('-')
    if not safe:
        safe = 'resource'
    if len(safe) > maxlen:
        h = hashlib.sha1(safe.encode('utf-8')).hexdigest()[:6]
        safe = safe[: maxlen - 7].rstrip('-') + '-' + h
    return safe

def format_date(dt_str: str) -> str | None:
    if not dt_str:
        return None
    try:
        dt = datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
        return dt.strftime('%Y-%m-%dT%H:%M:%S')
    except ValueError:
        logger.warning(f"Could not parse date: {dt_str}")
        return None

def open_stream(spec: str, base_dir: Path) -> io.BytesIO:
    parts = spec.replace('\\', '/').split('!/')
    data = (base_dir / parts[0].lstrip('./')).read_bytes()
    for entry in parts[1:]:
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            data = zf.read(entry)
    buf = io.BytesIO(data)
    buf.seek(0)
    return buf

def safe_create_package(ckan, params: dict) -> dict:
    try:
        return ckan.action.package_create(**params)
    except ValidationError as e:
        errs = getattr(e, 'error_dict', {}) or {}
        name_errs = errs.get('name') or []
        if any('already exists' in msg for msg in name_errs):
            slug = params['name']
            logger.warning(f"Slug '{slug}' is in trash—purging and retrying…")
            try:
                ckan.action.dataset_purge(id=slug)
            except Exception as purge_exc:
                logger.error(f"Could not purge '{slug}': {purge_exc}")
                raise
            return ckan.action.package_create(**params)
        raise

# -----------------------------------------------------------------------------
# CKAN operations
# -----------------------------------------------------------------------------
def ensure_dataset(ckan, meta: dict, org_id: str | None) -> str:
    title       = meta.get('title') or meta.get('name') or Path(meta.get('upload','')).stem
    slug        = slugify(title)
    description = meta.get('description','')
    license_id  = meta.get('license_id','cc-BY')

    params = {
        'name':       slug,
        'title':      title,
        'notes':      description,
        'license_id': license_id,
    }
    if org_id:
        params['owner_org'] = org_id

    extras = {}
    for key in ('issued','modified','language','spatial','temporal','publisher'):
        val = meta.get(key)
        if not val:
            continue
        extras[f'dct_{key}'] = format_date(val) if key in ('issued','modified') else val
    if extras:
        params['extras'] = json.dumps(extras)

    if 'tags' in meta:
        params['tags'] = [{'name': t} for t in meta['tags']]
    if 'groups' in meta:
        params['groups'] = [{'name': g} for g in meta['groups']]

    try:
        pkg = ckan.action.package_show(id=slug)
        params['id'] = pkg['id']
        updated = ckan.action.package_update(**params)
        logger.info(f"↻ Dataset updated: {updated['id']}")
        return updated['id']
    except NotFound:
        created = safe_create_package(ckan, params)
        logger.info(f"✔ Dataset created: {created['id']}")
        return created['id']

def find_existing_resource(ckan, package_id: str, name: str) -> str | None:
    try:
        res = ckan.action.resource_search(filters={'package_id': package_id, 'name': name})
        results = res.get('results', [])
        return results[0]['id'] if results else None
    except Exception:
        return None

def delete_all_datasets(ckan):
    for slug in ckan.action.package_list():
        try:
            ckan.action.package_delete(id=slug)
            logger.info(f"✖ Soft-deleted dataset: {slug}")
        except Exception as e:
            logger.error(f"Failed soft-delete '{slug}': {e}")
        try:
            ckan.action.dataset_purge(id=slug)
            logger.info(f"🗑  Permanently purged dataset: {slug}")
        except Exception as e:
            logger.error(f"Failed purge '{slug}': {e}")

def delete_all_organizations(ckan):
    """
    Soft-delete then hard-purge every organization in CKAN.
    Requires sysadmin rights for organization_purge.
    """
    for org_slug in ckan.action.organization_list():
        try:
            ckan.action.organization_delete(id=org_slug)
            logger.info(f"✖ Soft-deleted organization: {org_slug}")
        except Exception as e:
            logger.error(f"Failed soft-delete org '{org_slug}': {e}")
        try:
            ckan.action.organization_purge(id=org_slug)
            logger.info(f"🗑  Permanently purged organization: {org_slug}")
        except Exception as e:
            logger.error(f"Failed purge org '{org_slug}': {e}")

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
if __name__ == '__main__':
    if not MANIFEST.exists():
        logger.error(f"Manifest not found at {MANIFEST}")
        sys.exit(1)
    base_dir = MANIFEST.parent

    ckan = RemoteCKAN(CKAN_URL, apikey=API_KEY)
    try:
        ckan.action.status_show()
    except Exception as e:
        logger.error(f"Cannot reach CKAN at {CKAN_URL}: {e}")
        sys.exit(1)

    # --- Interactive prompts ---

    # 1. Delete all organizations?
    if input("Delete ALL existing organizations? Type 'yes' to confirm: ").strip().lower() == 'yes':
        delete_all_organizations(ckan)

    # 2. Delete all datasets?
    if input("Delete ALL existing datasets? Type 'yes' to confirm: ").strip().lower() == 'yes':
        delete_all_datasets(ckan)

    # 3. Enter new organization name (leave blank for none)
    org_input = input("Enter organization name for new datasets: ").strip()
    org_slug = slugify(org_input) if org_input else None

    # 4. Create or fetch the new organization
    org_id = None
    if org_input:
        try:
            ckan.action.organization_show(id=org_slug)
            org_id = org_slug
            logger.info(f"Using existing organization: {org_id}")
        except NotFound:
            org = ckan.action.organization_create(name=org_slug, title=org_input)
            org_id = org['id']
            logger.info(f"✔ Organization created: {org_id}")

    # 5. Single vs separate datasets
    separate = input("Create separate dataset per file? Type 'yes' for separate, anything else for single: ").strip().lower() == 'yes'

    # Load manifest
    manifest = json.loads(MANIFEST.read_text(encoding='utf-8'))
    resources = manifest.get('resources', [])
    ds_meta   = manifest.get('dataset', {})

    # Upload logic
    if not separate:
        ds_id = ensure_dataset(ckan, ds_meta, org_id)
        for res in tqdm(resources, desc="Uploading"):
            try:
                stream   = open_stream(res['upload'], base_dir)
                fname    = res.get('extras', {}).get('original_filename', Path(res['upload']).name)
                slug      = slugify(fname)
                created   = format_date(res.get('created',''))
                modified  = format_date(res.get('last_modified',''))
                data      = stream.read()
                solr_url  = f"{CKAN_URL}dataset/{ds_id}/resource/{{id}}/preview"
                extras    = {**res.get('extras', {}), 'view_url': solr_url}

                payload = {
                    'package_id':    ds_id,
                    'name':          slug,
                    'description':   res.get('description',''),
                    'format':        res['format'],
                    'mimetype':      res['mimetype'],
                    'upload':        io.BytesIO(data),
                    'extras':        json.dumps(extras),
                    **({'created': created}   if created  else {}),
                    **({'last_modified': modified} if modified else {}),
                }

                existing = find_existing_resource(ckan, ds_id, slug)
                if existing:
                    payload['id'] = existing
                    out = ckan.action.resource_update(**payload)
                    logger.info(f"↻ Updated: {fname} → {out['url']}")
                else:
                    out = ckan.action.resource_create(**payload)
                    logger.info(f"✔ Added:   {fname} → {out['url']}")
            except Exception as e:
                logger.exception(f"✘ Error processing {res.get('upload','<unknown>')}: {e}")
    else:
        for res in tqdm(resources, desc="Processing files"):
            try:
                title = res.get('extras', {}).get('original_filename', Path(res['upload']).name)
                file_meta = {
                    'title':       title,
                    'description': res.get('description',''),
                    'license_id':  ds_meta.get('license_id','cc-BY'),
                }
                ds_id = ensure_dataset(ckan, file_meta, org_id)

                stream   = open_stream(res['upload'], base_dir)
                slug      = slugify(title)
                created   = format_date(res.get('created',''))
                modified  = format_date(res.get('last_modified',''))
                data      = stream.read()
                solr_url  = f"{CKAN_URL}dataset/{ds_id}/resource/{{id}}/preview"
                extras    = {**res.get('extras', {}), 'view_url': solr_url}

                payload = {
                    'package_id':    ds_id,
                    'name':          slug,
                    'description':   res.get('description',''),
                    'format':        res['format'],
                    'mimetype':      res['mimetype'],
                    'upload':        io.BytesIO(data),
                    'extras':        json.dumps(extras),
                    **({'created': created}   if created  else {}),
                    **({'last_modified': modified} if modified else {}),
                }

                existing = find_existing_resource(ckan, ds_id, slug)
                if existing:
                    payload['id'] = existing
                    out = ckan.action.resource_update(**payload)
                    logger.info(f"↻ Updated resource in {ds_id}: {title} → {out['url']}")
                else:
                    out = ckan.action.resource_create(**payload)
                    logger.info(f"✔ Created resource in {ds_id}: {title} → {out['url']}")
            except Exception as e:
                logger.exception(f"✘ Error processing {res.get('upload','<unknown>')}: {e}")

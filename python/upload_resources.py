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
API_KEY   = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJqdGkiOiJmZDRjUEVrQkVRaVRNNXJ3VUMtYVY4N0R1YlhzeTlZMmlrZlpVa0VRRVJnIiwiaWF0IjoxNzUwMzM3MDQ1fQ.kXvgCvs7Emc7RfPxGZ1znLz7itMqK4p0hXYoEoc8LaA"
MANIFEST  = Path(__file__).resolve().parent.parent / "report.json"

warnings.filterwarnings("ignore", "pkg_resources is deprecated", UserWarning)

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Utils
# -----------------------------------------------------------------------------
def slugify(text: str, maxlen: int = 100) -> str:
    stem = Path(text).stem.lower()
    safe = re.sub(r'[^a-z0-9\-_]+', '-', stem).strip('-')
    if not safe:
        safe = 'resource'
    if len(safe) > maxlen:
        h = hashlib.sha1(safe.encode()).hexdigest()[:6]
        safe = safe[:maxlen-7].rstrip('-') + '-' + h
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
    parts = spec.replace('\\','/').split('!/')
    data = (base_dir / parts[0].lstrip('./')).read_bytes()
    for entry in parts[1:]:
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            data = zf.read(entry)
    buf = io.BytesIO(data); buf.seek(0)
    return buf

def safe_create_package(ckan, params: dict) -> dict:
    try:
        return ckan.action.package_create(**params)
    except ValidationError as e:
        errs = getattr(e, 'error_dict', {}) or {}
        name_errs = errs.get('name') or []
        if any('already exists' in msg for msg in name_errs):
            slug = params['name']
            logger.warning(f"Slug '{slug}' is trashed—purging and retrying…")
            # dataset_purge also clears resources
            try:
                ckan.action.dataset_purge(id=slug)
            except Exception:
                pass
            return ckan.action.package_create(**params)
        raise

# -----------------------------------------------------------------------------
# Deletion helpers
# -----------------------------------------------------------------------------
def delete_all_datasets(ckan):
    """
    For every dataset: soft-delete + hard-purge (which also purges its resources).
    """
    for ds_slug in ckan.action.package_list():
        logger.info(f"- Dataset: {ds_slug}")
        try:
            ckan.action.package_delete(id=ds_slug)
            logger.info(f"  • Deleted: {ds_slug}")
        except Exception as e:
            logger.error(f"  • Failed delete: {e}")
        try:
            ckan.action.dataset_purge(id=ds_slug)
            logger.info(f"  • Purged:  {ds_slug}")
        except Exception:
            # it may not exist or not be supported—ignore
            pass

def delete_all_organizations(ckan):
    """
    For every organization: wipe datasets, then soft-delete + purge the org itself.
    """
    for org_slug in ckan.action.organization_list():
        logger.info(f"- Organization: {org_slug}")
        delete_all_datasets(ckan)
        try:
            ckan.action.organization_delete(id=org_slug)
            logger.info(f"  • Deleted org: {org_slug}")
        except Exception as e:
            logger.error(f"  • Failed org-delete: {e}")
        try:
            ckan.action.organization_purge(id=org_slug)
            logger.info(f"  • Purged org:  {org_slug}")
        except Exception:
            pass

# -----------------------------------------------------------------------------
# CKAN operations
# -----------------------------------------------------------------------------
def ensure_dataset(ckan, meta: dict, org_id: str | None) -> str:
    """
    Create or update a CKAN dataset from metadata.
    If an existing dataset is soft-deleted, purge then re-create.
    """
    title       = meta.get('title') or Path(meta.get('upload','')).stem
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

    # DCAT extras
    extras = {}
    for key in ('issued','modified','language','spatial','temporal','publisher'):
        v = meta.get(key)
        if v:
            extras[f'dct_{key}'] = format_date(v) if key in ('issued','modified') else v
    if extras:
        params['extras'] = json.dumps(extras)
    if 'tags' in meta:
        params['tags'] = [{'name': t} for t in meta['tags']]
    if 'groups' in meta:
        params['groups'] = [{'name': g} for g in meta['groups']]

    try:
        pkg = ckan.action.package_show(id=slug)
        if pkg.get('state') == 'deleted':
            logger.info(f"⚠ Found trashed dataset '{slug}', purging…")
            try:
                ckan.action.dataset_purge(id=slug)
            except Exception:
                pass
            created = safe_create_package(ckan, params)
            logger.info(f"✔ Re-created: {created['id']}")
            return created['id']

        # update existing
        params['id'] = pkg['id']
        updated = ckan.action.package_update(**params)
        logger.info(f"↻ Updated:    {updated['id']}")
        return updated['id']

    except NotFound:
        created = safe_create_package(ckan, params)
        logger.info(f"✔ Created:    {created['id']}")
        return created['id']

def find_existing_resource(ckan, pkg_id: str, name: str) -> str | None:
    try:
        res = ckan.action.resource_search(filters={'package_id': pkg_id, 'name': name})
        return res['results'][0]['id'] if res.get('results') else None
    except Exception:
        return None

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
if __name__ == '__main__':
    # verify manifest
    if not MANIFEST.exists():
        logger.error(f"Manifest not found at {MANIFEST}")
        sys.exit(1)
    base_dir = MANIFEST.parent

    # connect
    ckan = RemoteCKAN(CKAN_URL, apikey=API_KEY)
    try:
        ckan.action.status_show()
    except Exception as e:
        logger.error(f"Cannot reach CKAN: {e}")
        sys.exit(1)

    # --- Questions ---
    print("\n=== CONFIGURATION ===")
    wipe_orgs  = input("Delete ALL organizations? Type 'yes' to confirm: ").strip().lower() == 'yes'
    wipe_ds    = input("Delete ALL datasets?      Type 'yes' to confirm: ").strip().lower() == 'yes'
    org_input  = input("Enter new organization name (leave blank for none): ").strip()
    separate   = input("Separate dataset per file? Type 'yes' for separate: ").strip().lower() == 'yes'
    print("======================\n")

    # --- Deletion ---
    if wipe_orgs:
        print(">> Deleting all organizations:")
        delete_all_organizations(ckan)
        print()
    if wipe_ds:
        print(">> Deleting all datasets:")
        delete_all_datasets(ckan)
        print()

    # --- Organization creation ---
    org_id = None
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

    # --- Upload ---
    manifest  = json.loads(MANIFEST.read_text(encoding='utf-8'))
    resources = manifest.get('resources', [])
    ds_meta   = manifest.get('dataset', {})

    mode = "per-file datasets" if separate else "single dataset"
    print(f">> Uploading in {mode} mode\n")

    if not separate:
        ds_id = ensure_dataset(ckan, ds_meta, org_id)
        print(f"→ Using dataset: {ds_id}\n")
        for res in tqdm(resources, desc="Uploading"):
            stream   = open_stream(res['upload'], base_dir)
            fname    = res.get('extras',{}).get('original_filename', Path(res['upload']).name)
            rslug    = slugify(fname)
            created  = format_date(res.get('created',''))
            modified = format_date(res.get('last_modified',''))
            data     = stream.read()
            solr     = f"{CKAN_URL}dataset/{ds_id}/resource/{{id}}/preview"
            extras   = {**res.get('extras',{}), 'view_url': solr}

            payload = {
                'package_id':    ds_id,
                'name':          rslug,
                'description':   res.get('description',''),
                'format':        res['format'],
                'mimetype':      res['mimetype'],
                'upload':        io.BytesIO(data),
                'extras':        json.dumps(extras),
                **({'created': created}    if created  else {}),
                **({'last_modified': modified} if modified else {}),
            }

            existing = find_existing_resource(ckan, ds_id, rslug)
            if existing:
                payload['id'] = existing
                out = ckan.action.resource_update(**payload)
                logger.info(f"↻ Updated: {fname} → {out['url']}")
            else:
                out = ckan.action.resource_create(**payload)
                logger.info(f"✔ Added:   {fname} → {out['url']}")
    else:
        for res in tqdm(resources, desc="Per-file datasets"):
            title    = res.get('extras',{}).get('original_filename', Path(res['upload']).name)
            file_md  = {'title': title, 'description': res.get('description',''), 'license_id': ds_meta.get('license_id','cc-BY')}
            ds_id    = ensure_dataset(ckan, file_md, org_id)
            print(f"\n→ Dataset: {ds_id} ({title})")

            stream   = open_stream(res['upload'], base_dir)
            rslug    = slugify(title)
            created  = format_date(res.get('created',''))
            modified = format_date(res.get('last_modified',''))
            data     = stream.read()
            solr     = f"{CKAN_URL}dataset/{ds_id}/resource/{{id}}/preview"
            extras   = {**res.get('extras',{}), 'view_url': solr}

            payload = {
                'package_id':    ds_id,
                'name':          rslug,
                'description':   res.get('description',''),
                'format':        res['format'],
                'mimetype':      res['mimetype'],
                'upload':        io.BytesIO(data),
                'extras':        json.dumps(extras),
                **({'created': created}    if created  else {}),
                **({'last_modified': modified} if modified else {}),
            }

            existing = find_existing_resource(ckan, ds_id, rslug)
            if existing:
                payload['id'] = existing
                out = ckan.action.resource_update(**payload)
                logger.info(f"↻ Updated: {title} → {out['url']}")
            else:
                out = ckan.action.resource_create(**payload)
                logger.info(f"✔ Created: {title} → {out['url']}")

#!/usr/bin/env python3
"""
CKAN Uploader Script with optional per-resource datasets:
- Top-level keys are packageIds, mapping to resource dicts.
- Creates one organization per packageId.
- Depending on user input, creates either:
    a) one dataset per package containing all its resources, or
    b) one separate dataset per resource under the same org.
- Structured logging; correct path handling.
"""
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
from typing import Optional, Dict, Any, Union, List

from ckanapi import RemoteCKAN, NotFound, ValidationError, NotAuthorized
from tqdm import tqdm

# -----------------------------------------------------------------------------
# Configuration Constants
# -----------------------------------------------------------------------------
CKAN_URL: str = "https://special-space-disco-94v44prrppr36q6-5000.app.github.dev/"
API_KEY: str = (
    # Truncated API key; replace with a valid key
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJqdGkiOiJmZDRjUEVrQkVRaVRNNXJ3VUMtYVY4N0R1YlhzeTlZMmlrZlpVa0VRRVJnIiwiaWF0IjoxNzUwMzM3MDQ1fQ.kXvgCvs7Emc7RfPxGZ1znLz7itMqK4p0hXYoEoc8LaA"
)
# Project layout: root contains 'document/' and 'reports/'
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DOC_ROOT = PROJECT_ROOT / "document"
MANIFEST = PROJECT_ROOT / "reports" / "all-reports.json"

warnings.filterwarnings("ignore", "pkg_resources is deprecated", UserWarning)

LOG_FORMAT = "[%(asctime)s] %(levelname)-8s %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Utility Functions
# -----------------------------------------------------------------------------
def slugify(text: str, maxlen: int = 100) -> str:
    stem = Path(text).stem.lower()
    slug = re.sub(r'[^a-z0-9\-_]+', '-', stem).strip('-') or 'item'
    if len(slug) > maxlen:
        suffix = hashlib.sha1(slug.encode()).hexdigest()[:6]
        slug = f"{slug[:maxlen-7].rstrip('-')}-{suffix}"
    return slug


def format_date(dt_str: Union[str, None]) -> Optional[str]:
    if not dt_str:
        return None
    try:
        dt = datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
        return dt.strftime('%Y-%m-%dT%H:%M:%S')
    except ValueError:
        logger.warning("Invalid date '%s', skipping.", dt_str)
        return None


def open_stream(spec: str) -> io.BytesIO:
    parts = spec.replace('\\', '/').split('!/')
    rel = parts[0].lstrip('./')
    file_path = PROJECT_ROOT / rel
    data = file_path.read_bytes()
    for entry in parts[1:]:
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            data = zf.read(entry)
    buf = io.BytesIO(data)
    buf.seek(0)
    return buf


def safe_create_package(ckan: RemoteCKAN, params: Dict[str, Any]) -> Dict[str, Any]:
    try:
        return ckan.action.package_create(**params)
    except ValidationError as exc:
        errors = getattr(exc, 'error_dict', {}) or {}
        if any('already exists' in msg for msg in errors.get('name', [])):
            slug = params['name']
            logger.warning("Slug '%s' trashed—purging...", slug)
            try:
                ckan.action.dataset_purge(id=slug)
            except Exception:
                logger.error("Failed to purge '%s'", slug)
            return ckan.action.package_create(**params)
        raise

# -----------------------------------------------------------------------------
# Deletion Helpers
# -----------------------------------------------------------------------------
def delete_all_datasets(ckan: RemoteCKAN) -> None:
    for slug in ckan.action.package_list():
        try:
            ckan.action.package_delete(id=slug)
            ckan.action.dataset_purge(id=slug)
        except NotAuthorized as e:
            logger.error("Permission denied deleting '%s': %s", slug, e)
            sys.exit(1)
        except Exception as e:
            logger.error("Error deleting '%s': %s", slug, e)


def delete_all_organizations(ckan: RemoteCKAN) -> None:
    for slug in ckan.action.organization_list():
        delete_all_datasets(ckan)
        try:
            ckan.action.organization_delete(id=slug)
            ckan.action.organization_purge(id=slug)
        except NotAuthorized as e:
            logger.error("Permission denied org '%s': %s", slug, e)
            sys.exit(1)
        except Exception as e:
            logger.error("Error org '%s': %s", slug, e)

# -----------------------------------------------------------------------------
# Dataset & Resource Operations
# -----------------------------------------------------------------------------
def ensure_dataset(ckan: RemoteCKAN, meta: Dict[str, Any], org_id: Optional[str]) -> str:
    title = meta['title']
    slug = slugify(title)
    params = {'name': slug, 'title': title, 'notes': meta.get('description',''), 'license_id': meta.get('license_id','cc-BY')}
    if org_id:
        params['owner_org'] = org_id
    try:
        pkg = ckan.action.package_show(id=slug)
        if pkg.get('state') == 'deleted':
            ckan.action.dataset_purge(id=slug)
            pkg = safe_create_package(ckan, params)
        else:
            params['id'] = pkg['id']
            pkg = ckan.action.package_update(**params)
        return pkg['id']
    except NotFound:
        created = safe_create_package(ckan, params)
        return created['id']


def find_existing_resource(ckan: RemoteCKAN, pkg_id: str, name: str) -> Optional[str]:
    try:
        res = ckan.action.resource_search(filters={'package_id': pkg_id, 'name': name})
        return res['results'][0]['id'] if res.get('results') else None
    except Exception:
        return None

# -----------------------------------------------------------------------------
# CKAN Connection
# -----------------------------------------------------------------------------
def connect_ckan() -> RemoteCKAN:
    try:
        ckan = RemoteCKAN(CKAN_URL, apikey=API_KEY)
        ckan.action.status_show()
        return ckan
    except NotAuthorized:
        logger.error("Invalid API key or insufficient permissions")
        sys.exit(1)
    except Exception as e:
        logger.error("Connection failed: %s", e)
        sys.exit(1)

# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------
def main() -> None:
    logger.info("=== Starting upload_resources.py ===")
    if not MANIFEST.exists():
        logger.error("Manifest not found: %s", MANIFEST)
        sys.exit(1)

    ckan = connect_ckan()

    if input("Delete ALL organizations? (yes/no): ").strip().lower() == 'yes':
        delete_all_organizations(ckan)
    if input("Delete ALL datasets? (yes/no): ").strip().lower() == 'yes':
        delete_all_datasets(ckan)

    # Ask per-resource dataset
    per_resource = input("Separate dataset for each resource? (yes/no): ").strip().lower() == 'yes'

    manifest_data = json.loads(MANIFEST.read_text(encoding='utf-8'))

    for package_id, resources in manifest_data.items():
        org_slug = slugify(package_id)
        try:
            ckan.action.organization_show(id=org_slug)
            logger.info("Using existing org: %s", org_slug)
        except NotFound:
            org = ckan.action.organization_create(name=org_slug, title=package_id)
            org_slug = org['id']
            logger.info("Created org: %s", org_slug)

        if not per_resource:
            # single dataset per package
            ds_meta = {'title': package_id, 'description': ''}
            ds_id = ensure_dataset(ckan, ds_meta, org_slug)
            logger.info("Dataset %s → %s", package_id, ds_id)
            for res in tqdm(resources, desc=f"Uploading resources for {package_id}"):
                path_spec = res['upload']
                stream = open_stream(path_spec)
                filename = Path(path_spec).name
                rslug = slugify(filename)
                payload = {'package_id': ds_id, 'name': rslug,
                           'description': res.get('description',''),
                           'format': res.get('format',''), 'mimetype': res.get('mimetype',''),
                           'upload': io.BytesIO(stream.read())}
                existing = find_existing_resource(ckan, ds_id, rslug)
                if existing:
                    payload['id'] = existing
                    out = ckan.action.resource_update(**payload)
                    logger.info("Updated resource: %s", out['url'])
                else:
                    out = ckan.action.resource_create(**payload)
                    logger.info("Created resource: %s", out['url'])
        else:
            # one dataset per resource
            for res in tqdm(resources, desc=f"Creating datasets for {package_id}"):
                filename = Path(res['upload']).name
                ds_meta = {'title': filename, 'description': res.get('description','')}
                ds_id = ensure_dataset(ckan, ds_meta, org_slug)
                logger.info("Dataset for %s → %s", filename, ds_id)
                path_spec = res['upload']
                stream = open_stream(path_spec)
                rslug = slugify(filename)
                payload = {'package_id': ds_id, 'name': rslug,
                           'description': res.get('description',''),
                           'format': res.get('format',''), 'mimetype': res.get('mimetype',''),
                           'upload': io.BytesIO(stream.read())}
                existing = find_existing_resource(ckan, ds_id, rslug)
                if existing:
                    payload['id'] = existing
                    out = ckan.action.resource_update(**payload)
                    logger.info("Updated resource: %s", out['url'])
                else:
                    out = ckan.action.resource_create(**payload)
                    logger.info("Created resource: %s", out['url'])

if __name__ == '__main__':
    main()

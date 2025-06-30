#!/usr/bin/env python3
"""
CKAN Uploader Script with optional per-resource datasets:
- Top-level keys are packageIds, mapping to resource dicts.
- Creates one organization per packageId.
- Depending on user input, creates either:
    a) one dataset per package containing all its resources, or
    b) one separate dataset per resource under the same org.
- Structured logging; dataset metadata now includes resource formats and counts.
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
from typing import Optional, Dict, Any, Union, List

from ckanapi import RemoteCKAN, NotFound
from tqdm import tqdm

# -----------------------------------------------------------------------------
# Configuration Constants
# -----------------------------------------------------------------------------
CKAN_URL = "https://special-space-disco-94v44prrppr36q6-5000.app.github.dev/"
API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJqdGkiOiJmZDRjUEVrQkVRaVRNNXJ3VUMtYVY4N0R1YlhzeTlZMmlrZlpVa0VRRVJnIiwiaWF0IjoxNzUwMzM3MDQ1fQ.kXvgCvs7Emc7RfPxGZ1znLz7itMqK4p0hXYoEoc8LaA"
PROJECT_ROOT = Path(__file__).resolve().parent.parent
MANIFEST = PROJECT_ROOT / "reports" / "all-reports.json"

warnings.filterwarnings("ignore", "pkg_resources is deprecated", UserWarning)

# Logging without timestamps
logging.basicConfig(level=logging.INFO, format="%(levelname)-8s %(message)s")
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


def open_stream(spec: str) -> io.BytesIO:
    parts = spec.replace('\\', '/').split('!/')
    rel = parts[0].lstrip('./')
    data = (PROJECT_ROOT / rel).read_bytes()
    for entry in parts[1:]:
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            data = zf.read(entry)
    buf = io.BytesIO(data)
    buf.seek(0)
    return buf

# -----------------------------------------------------------------------------
# CKAN Helpers
# -----------------------------------------------------------------------------
def connect_ckan() -> RemoteCKAN:
    logger.info("Connecting to CKAN at %s", CKAN_URL)
    try:
        ckan = RemoteCKAN(CKAN_URL, apikey=API_KEY)
        ckan.action.status_show()
        logger.info("Connection OK")
        return ckan
    except Exception as e:
        logger.error("Failed to connect/authenticate: %s", e)
        sys.exit(1)


def ensure_organization(ckan: RemoteCKAN, org_name: str) -> str:
    slug = slugify(org_name)
    try:
        ckan.action.organization_show(id=slug)
        logger.info("Org exists: %s", slug)
    except NotFound:
        org = ckan.action.organization_create(name=slug, title=org_name)
        slug = org['id']
        logger.info("Created org: %s", slug)
    return slug


def ensure_dataset(
    ckan: RemoteCKAN,
    title: str,
    description: str,
    org_id: str,
    resource_formats: Optional[List[str]] = None,
    resource_count: Optional[int] = None
) -> str:
    slug = slugify(title)
    params: Dict[str, Any] = {
        'name': slug,
        'title': title,
        'notes': description,
        'owner_org': org_id,
        'type': 'dataset'
    }
    # Build extras as list of dicts (CKAN expects list of key/value pairs)
    extras_items: List[Dict[str, str]] = []
    if resource_formats:
        extras_items.append({'key': 'resource_formats', 'value': ','.join(sorted(set(resource_formats)))})
    if resource_count is not None:
        extras_items.append({'key': 'resource_count', 'value': str(resource_count)})
    if extras_items:
        params['extras'] = extras_items

    try:
        pkg = ckan.action.package_show(id=slug)
        logger.info("Updating dataset: %s", slug)
        params['id'] = pkg['id']
        pkg = ckan.action.package_update(**params)
    except NotFound:
        logger.info("Creating dataset: %s", slug)
        pkg = ckan.action.package_create(**params)

    return pkg['id']


def upload_resource(
    ckan: RemoteCKAN,
    ds_id: str,
    filename: str,
    stream: io.BytesIO,
    resource_format: str,
    mimetype: str
) -> None:
    rslug = slugify(filename)
    payload: Dict[str, Any] = {
        'package_id': ds_id,
        'name': rslug,
        'format': resource_format,
        'mimetype': mimetype,
        'upload': stream
    }
    existing = None
    try:
        res = ckan.action.resource_search(filters={'package_id': ds_id, 'name': rslug})
        if res.get('results'):
            existing = res['results'][0]['id']
    except Exception:
        pass

    if existing:
        payload['id'] = existing
        result = ckan.action.resource_update(**payload)
        logger.info("• Updated resource '%s' (%s)", filename, result.get('url',''))
    else:
        result = ckan.action.resource_create(**payload)
        logger.info("• Created resource '%s' (%s)", filename, result.get('url',''))

# -----------------------------------------------------------------------------
# Main Execution
# -----------------------------------------------------------------------------

def main():
    logger.info("=== upload_resources.py starting ===")
    if not MANIFEST.exists():
        logger.error("Manifest missing: %s", MANIFEST)
        sys.exit(1)

    ckan = connect_ckan()
    if input("Delete ALL organizations? (yes/no): ").lower().startswith('y'):
        for org in ckan.action.organization_list():
            for pkg in ckan.action.package_list(owner_org=org):
                ckan.action.package_delete(id=pkg)
                ckan.action.dataset_purge(id=pkg)
            ckan.action.organization_delete(id=org)
            ckan.action.organization_purge(id=org)
        logger.info("All orgs and datasets deleted.")

    per_resource = input("Separate dataset per resource? (yes/no): ").lower().startswith('y')
    data = json.loads(MANIFEST.read_text())

    for pkg_name, resources in data.items():
        logger.info("\n==== Package: %s ===", pkg_name)
        org_id = ensure_organization(ckan, pkg_name)

        if not per_resource:
            formats = [res.get('format','') for res in resources]
            ds_id = ensure_dataset(
                ckan, pkg_name, '', org_id,
                resource_formats=formats,
                resource_count=len(resources)
            )
            logger.info("-- Uploading %d resources to dataset '%s' --", len(resources), pkg_name)
            for res in tqdm(resources, desc="Resources"):
                filename = Path(res['upload']).name
                stream = open_stream(res['upload'])
                upload_resource(
                    ckan, ds_id, filename,
                    stream, res.get('format',''), res.get('mimetype','')
                )

        else:
            for res in resources:
                filename = Path(res['upload']).name
                format_ = res.get('format','')
                ds_id = ensure_dataset(
                    ckan, filename, res.get('description',''), org_id,
                    resource_formats=[format_], resource_count=1
                )
                logger.info("-- Uploading single resource '%s' to its own dataset --", filename)
                stream = open_stream(res['upload'])
                upload_resource(
                    ckan, ds_id, filename,
                    stream, format_, res.get('mimetype','')
                )

        logger.info("Finished package '%s' \n", pkg_name)

if __name__ == '__main__':
    main()

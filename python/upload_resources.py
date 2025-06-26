#!/usr/bin/env python3
"""
Refactored CKAN Uploader Script with Structured Logging
Full implementation without placeholders, handling both single and per-file modes.
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
from typing import Optional, Dict, Any, List, Union

from ckanapi import RemoteCKAN, NotFound, ValidationError, NotAuthorized
from tqdm import tqdm

# -----------------------------------------------------------------------------
# Configuration Constants
# -----------------------------------------------------------------------------
CKAN_URL: str = "https://special-space-disco-94v44prrppr36q6-5000.app.github.dev/"
API_KEY: str = (
    # Truncated API key; replace with a valid key
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
)
MANIFEST: Path = Path(__file__).resolve().parent.parent / "report.json"

# Suppress deprecation warnings
warnings.filterwarnings("ignore", "pkg_resources is deprecated", UserWarning)

# Configure structured logging
LOG_FORMAT = "[%(asctime)s] %(levelname)-8s %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Utility Functions
# -----------------------------------------------------------------------------
def slugify(text: str, maxlen: int = 100) -> str:
    """Generate a URL-safe slug from text."""
    stem = Path(text).stem.lower()
    slug = re.sub(r'[^a-z0-9\-_]+', '-', stem).strip('-') or 'resource'
    if len(slug) > maxlen:
        suffix = hashlib.sha1(slug.encode()).hexdigest()[:6]
        slug = f"{slug[:maxlen-7].rstrip('-')}-{suffix}"
    return slug


def format_date(dt_str: Union[str, None]) -> Optional[str]:
    """Convert ISO8601 to CKAN format; warn on parse failure."""
    if not dt_str:
        return None
    try:
        dt = datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
        return dt.strftime('%Y-%m-%dT%H:%M:%S')
    except ValueError:
        logger.warning("Invalid date '%s', skipping.", dt_str)
        return None


def open_stream(spec: str, base_dir: Path) -> io.BytesIO:
    """Open file or nested zip entry as byte stream."""
    parts = spec.replace('\\','/').split('!/')
    data = (base_dir / parts[0].lstrip('./')).read_bytes()
    for entry in parts[1:]:
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            data = zf.read(entry)
    buf = io.BytesIO(data)
    buf.seek(0)
    return buf


def safe_create_package(ckan: RemoteCKAN, params: Dict[str, Any]) -> Dict[str, Any]:
    """Create a dataset; purge trashed slug on conflict and retry."""
    try:
        return ckan.action.package_create(**params)
    except ValidationError as exc:
        errors = getattr(exc, 'error_dict', {}) or {}
        if any('already exists' in msg for msg in errors.get('name', [])):
            slug = params.get('name')
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
    logger.info("---- Deleting All Datasets ----")
    for slug in ckan.action.package_list():
        logger.info("Dataset: %s", slug)
        try:
            ckan.action.package_delete(id=slug)
            ckan.action.dataset_purge(id=slug)
            logger.info("  → Purged: %s", slug)
        except NotAuthorized as e:
            logger.error("  ✖ Permission denied deleting '%s': %s", slug, e)
            sys.exit(1)
        except Exception as e:
            logger.error("  ✖ Error deleting '%s': %s", slug, e)


def delete_all_organizations(ckan: RemoteCKAN) -> None:
    logger.info("---- Deleting All Organizations ----")
    for slug in ckan.action.organization_list():
        logger.info("Organization: %s", slug)
        delete_all_datasets(ckan)
        try:
            ckan.action.organization_delete(id=slug)
            ckan.action.organization_purge(id=slug)
            logger.info("  → Purged org: %s", slug)
        except NotAuthorized as e:
            logger.error("  ✖ Permission denied org '%s': %s", slug, e)
            sys.exit(1)
        except Exception as e:
            logger.error("  ✖ Error org '%s': %s", slug, e)

# -----------------------------------------------------------------------------
# Dataset & Resource Operations
# -----------------------------------------------------------------------------
def ensure_dataset(ckan: RemoteCKAN, meta: Dict[str, Any], org_id: Optional[str]) -> str:
    logger.info("---- Ensuring Dataset ----")
    title = meta.get('title') or Path(meta.get('upload','')).stem
    slug = slugify(title)
    logger.info("Title: %s | Slug: %s", title, slug)

    params = {'name':slug, 'title':title, 'notes':meta.get('description',''), 'license_id':meta.get('license_id','cc-BY')}
    if org_id: params['owner_org'] = org_id

    extras = {}
    for k in ('issued','modified','language','spatial','temporal','publisher'):
        if v:=meta.get(k):
            extras[f'dct_{k}'] = format_date(v) if k in ('issued','modified') else v
    if extras: params['extras'] = json.dumps(extras)
    if tags:=meta.get('tags'): params['tags'] = [{'name':t} for t in tags]
    if groups:=meta.get('groups'): params['groups'] = [{'name':g} for g in groups]

    try:
        pkg = ckan.action.package_show(id=slug)
        if pkg.get('state')=='deleted':
            logger.warning("Purging trashed dataset: %s", slug)
            ckan.action.dataset_purge(id=slug)
            pkg = safe_create_package(ckan, params)
            logger.info("Re-created ID: %s", pkg['id'])
        else:
            params['id'] = pkg['id']
            up = ckan.action.package_update(**params)
            logger.info("Updated ID: %s", up['id'])
            pkg = up
        return pkg['id']
    except NotFound:
        created = safe_create_package(ckan, params)
        logger.info("Created ID: %s", created['id'])
        return created['id']


def find_existing_resource(ckan: RemoteCKAN, pkg_id: str, name: str) -> Optional[str]:
    try:
        res = ckan.action.resource_search(filters={'package_id':pkg_id,'name':name})
        return res['results'][0]['id'] if res.get('results') else None
    except Exception:
        return None

# -----------------------------------------------------------------------------
# CKAN Connection & Permission Check
# -----------------------------------------------------------------------------
def connect_ckan() -> RemoteCKAN:
    logger.info("Connecting to CKAN at %s", CKAN_URL)
    try:
        ckan = RemoteCKAN(CKAN_URL, apikey=API_KEY)
        ckan.action.status_show()
        ckan.action.user_list()
        logger.info("Connection & permissions OK")
        return ckan
    except NotAuthorized:
        logger.error("ERROR: Invalid API key or insufficient permissions")
        sys.exit(1)
    except Exception as e:
        logger.error("ERROR: Connection failed: %s", e)
        sys.exit(1)

# -----------------------------------------------------------------------------
# Main Execution Flow
# -----------------------------------------------------------------------------
def main() -> None:
    logger.info("=== Starting upload_resources.py ===")
    if not MANIFEST.exists():
        logger.error("Manifest not found: %s", MANIFEST)
        sys.exit(1)
    base_dir = MANIFEST.parent

    ckan = connect_ckan()

    logger.info("--- Configuration ---")
    wipe_orgs = input("Delete ALL organizations? (yes/no): ").strip().lower()=='yes'
    wipe_ds = input("Delete ALL datasets? (yes/no): ").strip().lower()=='yes'
    org_input = input("Organization name (blank none): ").strip() or None
    separate = input("Separate dataset per file? (yes/no): ").strip().lower()=='yes'

    if wipe_orgs: delete_all_organizations(ckan)
    if wipe_ds: delete_all_datasets(ckan)

    org_id = None
    if org_input:
        slug = slugify(org_input)
        try:
            ckan.action.organization_show(id=slug)
            logger.info("Using existing org: %s",slug)
            org_id=slug
        except NotFound:
            org=ckan.action.organization_create(name=slug,title=org_input)
            logger.info("Created org: %s",org['id'])
            org_id=org['id']

    manifest_data = json.loads(MANIFEST.read_text(encoding='utf-8'))
    resources: List[Dict[str,Any]] = manifest_data.get('resources',[])
    ds_meta: Dict[str,Any] = manifest_data.get('dataset',{})

    mode_desc = 'per-file datasets' if separate else 'single dataset'
    logger.info("Mode: %s",mode_desc)

    if not separate:
        ds_id = ensure_dataset(ckan, ds_meta, org_id)
        logger.info("Active dataset ID: %s",ds_id)
        for res in tqdm(resources,desc="Uploading resources"):
            filename = res.get('extras',{}).get('original_filename',Path(res['upload']).name)
            logger.info("Processing: %s",filename)
            # Read and upload resource
            stream = open_stream(res['upload'], base_dir)
            rslug = slugify(filename)
            created = format_date(res.get('created'))
            modified = format_date(res.get('last_modified'))
            data_bytes = stream.read()
            view_url = f"{CKAN_URL}dataset/{ds_id}/resource/{{id}}/preview"
            extras = {**res.get('extras',{}),'view_url':view_url}
            payload = {'package_id':ds_id,'name':rslug,'description':res.get('description',''),'format':res['format'],'mimetype':res['mimetype'],'upload':io.BytesIO(data_bytes),'extras':json.dumps(extras)}
            if created: payload['created']=created
            if modified: payload['last_modified']=modified
            if eid:=find_existing_resource(ckan,ds_id,rslug): payload['id']=eid; out=ckan.action.resource_update(**payload); logger.info("  → Updated → %s",out['url'])
            else: out=ckan.action.resource_create(**payload); logger.info("  → Created → %s",out['url'])
    else:
        for res in tqdm(resources,desc="Per-file datasets"):
            filename = res.get('extras',{}).get('original_filename',Path(res['upload']).name)
            logger.info("Creating dataset for: %s",filename)
            file_meta = {'title':filename,'description':res.get('description',''),'license_id':ds_meta.get('license_id','cc-BY'),'upload':res['upload']}
            ds_id = ensure_dataset(ckan,file_meta,org_id)
            logger.info("Dataset ID: %s",ds_id)
            # Read and upload resource
            stream = open_stream(res['upload'], base_dir)
            rslug = slugify(filename)
            created = format_date(res.get('created'))
            modified = format_date(res.get('last_modified'))
            data_bytes = stream.read()
            view_url = f"{CKAN_URL}dataset/{ds_id}/resource/{{id}}/preview"
            extras = {**res.get('extras',{}),'view_url':view_url}
            payload = {'package_id':ds_id,'name':rslug,'description':res.get('description',''),'format':res['format'],'mimetype':res['mimetype'],'upload':io.BytesIO(data_bytes),'extras':json.dumps(extras)}
            if created: payload['created']=created
            if modified: payload['last_modified']=modified
            if eid:=find_existing_resource(ckan,ds_id,rslug): payload['id']=eid; out=ckan.action.resource_update(**payload); logger.info("  → Updated → %s",out['url'])
            else: out=ckan.action.resource_create(**payload); logger.info("  → Created → %s",out['url'])

if __name__=='__main__':
    main()

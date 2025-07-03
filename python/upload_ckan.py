#!/usr/bin/env python3
"""
CKAN Uploader
Automatically upload resources from a manifest to a CKAN instance,
organizing them into datasets and resources per user preference.
"""

import io
import json
import zipfile
import logging
import re
import hashlib
import sys
from pathlib import Path
from typing import Optional, Dict, Any, List
import argparse

from ckanapi import RemoteCKAN, NotFound
from tqdm import tqdm

# ------------------------------------------------------------------------------
# Configuration (hard-coded)
# ------------------------------------------------------------------------------
CKAN_URL         = "https://special-space-disco-94v44prrppr36q6-5000.app.github.dev/"
API_KEY          = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJqdGkiOiJmZDRjUEVrQkVRaVRNNXJ3VUMtYVY4N0R1YlhzeTlZMmlrZlpVa0VRRVJnIiwiaWF0IjoxNzUwMzM3MDQ1fQ.kXvgCvs7Emc7RfPxGZ1znLz7itMqK4p0hXYoEoc8LaA"
PROJECT_ROOT     = Path(__file__).resolve().parent.parent
DEFAULT_MANIFEST = PROJECT_ROOT / "reports" / "all-reports.json"

# ------------------------------------------------------------------------------
# Setup logging
# ------------------------------------------------------------------------------
def setup_logging(level: int) -> None:
    """Configure root logger format and level."""
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------
def slugify(text: str, maxlen: int = 100) -> str:
    """
    Convert a string into a CKAN-compatible slug:
      - lowercase alphanumerics, dashes or underscores only
      - truncate + hash suffix if too long
    """
    base = Path(text).stem.lower()
    slug = re.sub(r"[^a-z0-9_-]+", "-", base).strip("-") or "item"
    if len(slug) > maxlen:
        suffix = hashlib.sha1(slug.encode()).hexdigest()[:6]
        slug = f"{slug[: maxlen - 7].rstrip('-')}-{suffix}"
    return slug

def open_stream(spec: str, base_path: Path) -> io.BytesIO:
    """
    Read a file or nested ZIP entry into a BytesIO stream.

    Examples:
      "./data.zip!/file.csv"
      "/absolute/path/to/file.json"
    """
    parts = spec.replace("\\", "/").split("!/")
    data = (base_path / parts[0].lstrip("./")).read_bytes()
    for entry in parts[1:]:
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            data = zf.read(entry)
    buf = io.BytesIO(data)
    buf.seek(0)
    return buf

# ------------------------------------------------------------------------------
# CKAN API interaction
# ------------------------------------------------------------------------------
def connect_ckan(url: str, api_key: str) -> RemoteCKAN:
    """
    Establish and verify connection to CKAN.
    Exits on error.
    """
    logger = logging.getLogger("ckan.connect")
    logger.info("Connecting to CKAN at %s", url)
    try:
        client = RemoteCKAN(url, apikey=api_key)
        client.action.status_show()
        logger.info("Authentication successful")
        return client
    except Exception as e:
        logger.error("Failed to connect or authenticate: %s", e)
        sys.exit(1)

def ensure_organization(ckan: RemoteCKAN, title: str) -> str:
    """
    Ensure an organization exists (create if missing).
    Returns its CKAN ID (slug).
    """
    logger = logging.getLogger("ckan.org")
    org_id = slugify(title)
    try:
        ckan.action.organization_show(id=org_id)
        logger.debug("Organization exists: %s", org_id)
    except NotFound:
        org = ckan.action.organization_create(name=org_id, title=title)
        org_id = org["id"]
        logger.info("Created organization: %s", org_id)
    return org_id

def ensure_dataset(
    ckan: RemoteCKAN,
    title: str,
    description: str,
    owner_org: str,
    resource_formats: Optional[List[str]] = None,
    resource_count: Optional[int] = None
) -> str:
    """
    Create or update a dataset with metadata and extras.
    Returns the dataset ID.
    """
    logger = logging.getLogger("ckan.dataset")
    ds_name = slugify(title)
    params: Dict[str, Any] = {
        "name": ds_name,
        "title": title,
        "notes": description,
        "owner_org": owner_org,
        "type": "dataset",
    }
    extras = []
    if resource_formats:
        extras.append({
            "key": "resource_formats",
            "value": ",".join(sorted(set(resource_formats)))
        })
    if resource_count is not None:
        extras.append({
            "key": "resource_count",
            "value": str(resource_count)
        })
    if extras:
        params["extras"] = extras

    try:
        existing = ckan.action.package_show(id=ds_name)
        params["id"] = existing["id"]
        logger.info("Updating dataset: %s", ds_name)
        pkg = ckan.action.package_update(**params)
    except NotFound:
        logger.info("Creating dataset: %s", ds_name)
        pkg = ckan.action.package_create(**params)

    return pkg["id"]

def upload_resource(
    ckan: RemoteCKAN,
    package_id: str,
    filename: str,
    stream: io.BytesIO,
    resource_format: str,
    mimetype: str
) -> None:
    """
    Add or replace a resource in a dataset, preserving IDs on update.
    """
    logger = logging.getLogger("ckan.resource")
    res_name = slugify(filename)
    payload = {
        "package_id": package_id,
        "name": res_name,
        "format": resource_format,
        "mimetype": mimetype,
        "upload": stream,
    }

    # Try updating existing resource
    try:
        search = ckan.action.resource_search(
            filters={"package_id": package_id, "name": res_name}
        )
        if search["results"]:
            payload["id"] = search["results"][0]["id"]
            result = ckan.action.resource_update(**payload)
            logger.info("Updated resource '%s' → %s", filename, result.get("url", ""))
            return
    except Exception:
        logger.debug("No existing resource to update.")

    # Create new resource
    result = ckan.action.resource_create(**payload)
    logger.info("Created resource '%s' → %s", filename, result.get("url", ""))

def purge_all(ckan: RemoteCKAN) -> None:
    """Delete every organization and its datasets from CKAN."""
    logger = logging.getLogger("ckan.purge")
    for org in ckan.action.organization_list():
        for pkg in ckan.action.package_list(owner_org=org):
            ckan.action.package_delete(id=pkg)
            ckan.action.dataset_purge(id=pkg)
        ckan.action.organization_delete(id=org)
        ckan.action.organization_purge(id=org)
    logger.info("All organizations and datasets have been purged.")

# ------------------------------------------------------------------------------
# CLI
# ------------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    p = argparse.ArgumentParser(description="Upload resources to CKAN")
    p.add_argument(
        "--manifest", "-m",
        type=Path,
        default=DEFAULT_MANIFEST,
        help="Path to JSON manifest"
    )
    p.add_argument(
        "--per-resource",
        action="store_true",
        help="Create one dataset per individual resource"
    )
    p.add_argument(
        "--purge",
        action="store_true",
        help="Delete all existing orgs & datasets before upload"
    )
    p.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level"
    )
    return p.parse_args()

# ------------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------------
def main() -> None:
    args = parse_args()
    setup_logging(getattr(logging, args.log_level))

    logger = logging.getLogger("main")
    if not args.manifest.exists():
        logger.error("Manifest not found: %s", args.manifest)
        sys.exit(1)

    ckan = connect_ckan(CKAN_URL, API_KEY)
    base_path = args.manifest.parent

    if args.purge:
        logger.info("Purging existing data")
        purge_all(ckan)

    data = json.loads(args.manifest.read_text())

    for pkg_title, resources in data.items():
        logger.info("Processing package '%s'", pkg_title)
        org_id = ensure_organization(ckan, pkg_title)

        if not args.per_resource:
            formats = [res.get("format", "") for res in resources]
            ds_id = ensure_dataset(
                ckan,
                title=pkg_title,
                description="",
                owner_org=org_id,
                resource_formats=formats,
                resource_count=len(resources)
            )
            logger.info("Uploading %d resources to dataset '%s'", len(resources), ds_id)
            for res in tqdm(resources, desc="Resources"):
                fname  = Path(res["upload"]).name
                stream = open_stream(res["upload"], base_path)
                upload_resource(
                    ckan, ds_id, fname, stream,
                    res.get("format", ""), res.get("mimetype", "")
                )
        else:
            for res in tqdm(resources, desc="Resources"):
                fname  = Path(res["upload"]).name
                fmt    = res.get("format", "")
                ds_id  = ensure_dataset(
                    ckan,
                    title=fname,
                    description=res.get("description", ""),
                    owner_org=org_id,
                    resource_formats=[fmt],
                    resource_count=1
                )
                stream = open_stream(res["upload"], base_path)
                upload_resource(ckan, ds_id, fname, stream, fmt, res.get("mimetype", ""))

        logger.info("Finished package '%s'", pkg_title)

if __name__ == "__main__":
    main()

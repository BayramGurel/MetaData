#!/usr/bin/env python3
"""
CKAN Uploader Script

Dit script beheert automatisch de upload van resources naar een CKAN-instance.
Per packageId wordt een organisatie aangemaakt (of hergebruikt).
Er kan gekozen worden voor:
  1) Eén dataset per package met alle bijbehorende resources.
  2) Eén dataset per individuele resource binnen dezelfde organisatie.

Functionaliteit:
  - Interactieve prompts (optioneel via flags) voor volledige opschoning en upload-structuur.
  - Heldere logging (niveau instelbaar via --log-level).
  - Configuratie via CLI-argumenten (geen externe config/env-bestanden).

Voorbeeld gebruik:
  python ckan_uploader.py --url https://mijn-ckan.nl/ --api-key ABC123 \
      --manifest reports/all-reports.json --log-level DEBUG --per-resource
"""
import warnings
# Suppress deprecated pkg_resources warning from ckanapi
warnings.filterwarnings(
    "ignore",
    message=".*pkg_resources is deprecated.*",
    category=UserWarning,
    module="ckanapi.version"
)
import io
import json
import zipfile
import logging
import re
import hashlib
import sys
from pathlib import Path
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
import argparse

from ckanapi import RemoteCKAN, NotFound
from tqdm import tqdm

# -----------------------------------------------------------------------------
# Standaard configuratie
# -----------------------------------------------------------------------------
DEFAULT_CKAN_URL = "https://special-space-disco-94v44prrppr36q6-5000.app.github.dev/"
DEFAULT_API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJqdGkiOiJmZDRjUEVrQkVRaVRNNXJ3VUMtYVY4N0R1YlhzeTlZMmlrZlpVa0VRRVJnIiwiaWF0IjoxNzUwMzM3MDQ1fQ.kXvgCvs7Emc7RfPxGZ1znLz7itMqK4p0hXYoEoc8LaA"
DEFAULT_MANIFEST = Path(__file__).resolve().parent.parent / "reports" / "all-reports.json"

# -----------------------------------------------------------------------------
# Hulpfuncties
# -----------------------------------------------------------------------------
def slugify(text: str, maxlen: int = 100) -> str:
    """
    Converteer een tekst naar een CKAN-compatibele identifier (slug):
      - Alleen kleine letters, cijfers, streepjes en underscores.
      - Bij overschrijding van maxlen: verkort en voeg hash-suffix toe.
    """
    name = Path(text).stem.lower()
    slug = re.sub(r'[^a-z0-9\-_]+', '-', name).strip('-') or 'item'
    if len(slug) > maxlen:
        suffix = hashlib.sha1(slug.encode()).hexdigest()[:6]
        slug = f"{slug[:maxlen-7].rstrip('-')}-{suffix}"
    return slug


def open_stream(spec: str, project_root: Path) -> io.BytesIO:
    """
    Lees bestanden of geneste ZIP-entries in als een BytesIO-stream.

    Voorbeeld spec: "./data.zip!/bestand.csv" of een gewoon bestandspad.
    """
    parts = spec.replace('\\', '/').split('!/')
    path = parts[0].lstrip('./')
    data = (project_root / path).read_bytes()
    for entry in parts[1:]:
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            data = zf.read(entry)
    stream = io.BytesIO(data)
    stream.seek(0)
    return stream

# -----------------------------------------------------------------------------
# CKAN-interactie
# -----------------------------------------------------------------------------
@dataclass
class ResourceSpec:
    upload: str
    format: str
    mimetype: str
    description: str = ''

class CKANUploader:
    def __init__(
        self,
        ckan_url: str,
        api_key: str,
        manifest: Path,
        log_level: str = 'INFO'
    ):
        self.ckan_url = ckan_url
        self.api_key = api_key
        self.manifest = manifest
        self.project_root = Path(__file__).resolve().parent.parent
        logging.basicConfig(level=getattr(logging, log_level.upper(), logging.INFO),
                            format="%(levelname)-8s %(message)s")
        self.logger = logging.getLogger(__name__)
        warnings.filterwarnings("ignore", "pkg_resources is deprecated", UserWarning)
        self.client: Optional[RemoteCKAN] = None

    def connect(self) -> None:
        """
        Legt verbinding met de CKAN-server en verifieert de API-sleutel.
        Bij fouten wordt het script beëindigd.
        """
        self.logger.info("Verbinding maken met CKAN: %s", self.ckan_url)
        try:
            self.client = RemoteCKAN(self.ckan_url, apikey=self.api_key)
            self.client.action.status_show()
            self.logger.info("Authenticatie geslaagd")
        except Exception as e:
            self.logger.error("Kon niet verbinden of authenticeren: %s", e)
            sys.exit(1)

    def ensure_organization(self, org_name: str) -> str:
        """
        Controleert of een organisatie bestaat; maakt deze anders aan.
        Retourneert de slug (ID) van de organisatie.
        """
        slug = slugify(org_name)
        try:
            self.client.action.organization_show(id=slug)
            self.logger.info("Organisatie gevonden: %s", slug)
        except NotFound:
            org = self.client.action.organization_create(name=slug, title=org_name)
            slug = org['id']
            self.logger.info("Organisatie aangemaakt: %s", slug)
        return slug

    def ensure_dataset(
        self,
        title: str,
        description: str,
        org_id: str,
        resource_formats: Optional[List[str]] = None,
        resource_count: Optional[int] = None
    ) -> str:
        """
        Creëert of werkt een dataset bij met metadata en extras.
        """
        slug = slugify(title)
        params: Dict[str, Any] = {
            'name': slug,
            'title': title,
            'notes': description,
            'owner_org': org_id,
            'type': 'dataset'
        }
        extras: List[Dict[str, str]] = []
        if resource_formats:
            extras.append({'key': 'resource_formats', 'value': ','.join(sorted(set(resource_formats)))})
        if resource_count is not None:
            extras.append({'key': 'resource_count', 'value': str(resource_count)})
        if extras:
            params['extras'] = extras

        try:
            existing = self.client.action.package_show(id=slug)
            params['id'] = existing['id']
            self.logger.info("Dataset bijwerken: %s", slug)
            pkg = self.client.action.package_update(**params)
        except NotFound:
            self.logger.info("Dataset aanmaken: %s", slug)
            pkg = self.client.action.package_create(**params)
        return pkg['id']

    def upload_resource(
        self,
        ds_id: str,
        filename: str,
        stream: io.BytesIO,
        resource_format: str,
        mimetype: str
    ) -> None:
        """
        Uploadt of vervangt een resource in een dataset.
        """
        rslug = slugify(filename)
        payload = {
            'package_id': ds_id,
            'name': rslug,
            'format': resource_format,
            'mimetype': mimetype,
            'upload': stream
        }
        try:
            search = self.client.action.resource_search(filters={'package_id': ds_id, 'name': rslug})
            if search.get('results'):
                payload['id'] = search['results'][0]['id']
                result = self.client.action.resource_update(**payload)
                self.logger.info("Resource bijgewerkt: %s → %s", filename, result.get('url',''))
                return
        except Exception:
            pass
        result = self.client.action.resource_create(**payload)
        self.logger.info("Resource aangemaakt: %s → %s", filename, result.get('url',''))

    def cleanup(self) -> None:
        """
        Verwijdert alle organisaties en datasets van de CKAN-instance.
        """
        self.logger.info("Start opschoning: alle organisaties en datasets verwijderen")
        for org in self.client.action.organization_list():
            for pkg in self.client.action.package_list(owner_org=org):
                self.client.action.package_delete(id=pkg)
                self.client.action.dataset_purge(id=pkg)
            self.client.action.organization_delete(id=org)
            self.client.action.organization_purge(id=org)
        self.logger.info("Opschoning voltooid.")

    def run(self, cleanup_flag: bool, per_resource_flag: bool) -> None:
        """
        Hoofdprogramma: leest manifest, verbindt, (optioneel) opschonen en uploaden.
        """
        if not self.manifest.exists():
            self.logger.error("Manifest niet gevonden: %s", self.manifest)
            sys.exit(1)

        self.connect()

        # Optioneel opschonen
        if cleanup_flag:
            self.cleanup()
        else:
            if input("Alle organisaties en datasets verwijderen? (ja/nee): ").lower().startswith('j'):
                self.cleanup()

        # Upload-structuur bepalen
        if per_resource_flag:
            per_resource = True
        else:
            per_resource = input("Aparte dataset per resource? (ja/nee): ").lower().startswith('j')

        data = json.loads(self.manifest.read_text())
        for pkg_name, raw_resources in data.items():
            self.logger.info("-- Pakket verwerken: %s", pkg_name)
            org_id = self.ensure_organization(pkg_name)

            if not per_resource:
                formats = [r.get('format','') for r in raw_resources]
                ds_id = self.ensure_dataset(
                    title=pkg_name,
                    description='',
                    org_id=org_id,
                    resource_formats=formats,
                    resource_count=len(raw_resources)
                )
                self.logger.info(
                    "Upload van %d resources naar dataset '%s' gestart", len(raw_resources), pkg_name
                )
                for res in tqdm(raw_resources, desc="Resources"):
                    name = Path(res['upload']).name
                    stream = open_stream(res['upload'], self.project_root)
                    self.upload_resource(ds_id, name, stream, res.get('format',''), res.get('mimetype',''))
            else:
                for res in raw_resources:
                    name = Path(res['upload']).name
                    fmt = res.get('format','')
                    ds_id = self.ensure_dataset(
                        title=name,
                        description=res.get('description',''),
                        org_id=org_id,
                        resource_formats=[fmt],
                        resource_count=1
                    )
                    self.logger.info("Upload resource '%s' naar eigen dataset", name)
                    stream = open_stream(res['upload'], self.project_root)
                    self.upload_resource(ds_id, name, stream, fmt, res.get('mimetype',''))

            self.logger.info("-- Pakket voltooid: %s --", pkg_name)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='CKAN Uploader Script')
    parser.add_argument('--url', default=DEFAULT_CKAN_URL, help='Basis-URL van de CKAN-instance')
    parser.add_argument('--api-key', default=DEFAULT_API_KEY, help='API-sleutel voor CKAN')
    parser.add_argument('--manifest', type=Path, default=DEFAULT_MANIFEST, help='Pad naar manifest JSON')
    parser.add_argument('--cleanup', action='store_true', help='Opschonen zonder prompt')
    parser.add_argument('--per-resource', dest='per_resource', action='store_true',
                        help='Aparte dataset per resource zonder prompt')
    parser.add_argument('--log-level', default='INFO',
                        choices=['DEBUG','INFO','WARNING','ERROR','CRITICAL'],
                        help='Logniveau instellen')
    return parser.parse_args()


def main():
    args = parse_args()
    uploader = CKANUploader(
        ckan_url=args.url,
        api_key=args.api_key,
        manifest=args.manifest,
        log_level=args.log_level
    )
    uploader.run(cleanup_flag=args.cleanup, per_resource_flag=args.per_resource)


if __name__ == '__main__':
    main()

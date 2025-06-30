#!/usr/bin/env python3
# -----------------------------------------------------------------------------
# CKAN Uploader Script
# -----------------------------------------------------------------------------
# Dit script beheert automatisch de upload van resources naar een CKAN-instance,
# waarbij per packageId een organisatie wordt aangemaakt (of hergebruikt).
# Afhankelijk van de gebruikerstoepassing kan gekozen worden voor:
#   1) Eén dataset per package met alle bijbehorende resources.
#   2) Eén dataset per individuele resource binnen dezelfde organisatie.
# Bij elke stap wordt heldere logging uitgevoerd en worden dataset-metadata
# (aantal resources, formaten) bijgehouden.
# -----------------------------------------------------------------------------

import warnings
warnings.filterwarnings("ignore","pkg_resources is deprecated",UserWarning)

import io
import json
import zipfile
import logging
import re
import hashlib
import sys
from pathlib import Path
from typing import Optional, Dict, Any, List

from ckanapi import RemoteCKAN, NotFound
from tqdm import tqdm

# -----------------------------------------------------------------------------
# Configuratie
# -----------------------------------------------------------------------------
CKAN_URL = "https://special-space-disco-94v44prrppr36q6-5000.app.github.dev/"
# API KEY kan veranderen per gebruiker
API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJqdGkiOiJmZDRjUEVrQkVRaVRNNXJ3VUMtYVY4N0R1YlhzeTlZMmlrZlpVa0VRRVJnIiwiaWF0IjoxNzUwMzM3MDQ1fQ.kXvgCvs7Emc7RfPxGZ1znLz7itMqK4p0hXYoEoc8LaA"
PROJECT_ROOT = Path(__file__).resolve().parent.parent
MANIFEST = PROJECT_ROOT / "reports" / "all-reports.json"

# Logging configureren: alleen niveau en bericht
logging.basicConfig(level=logging.INFO, format="%(levelname)-8s %(message)s")
logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Hulpfuncties
# -----------------------------------------------------------------------------

def slugify(text: str, maxlen: int = 100) -> str:
    """
    Converteer een tekst naar een CKAN-compatibele identifier (slug):
      - Alleen kleine letters, cijfers, streepjes en underscores.
      - Bij overschrijding van maxlen: verkort en voeg hash-suffix toe.
    """
    naam = Path(text).stem.lower()
    slug = re.sub(r'[^a-z0-9\-_]+', '-', naam).strip('-') or 'item'
    if len(slug) > maxlen:
        hash_suffix = hashlib.sha1(slug.encode()).hexdigest()[:6]
        slug = f"{slug[:maxlen-7].rstrip('-')}-{hash_suffix}"
    return slug


def open_stream(spec: str) -> io.BytesIO:
    """
    Lees bestanden of geneste ZIP-entries in als een BytesIO-stream.

    Voorbeeld spec: "./data.zip!/bestand.csv" of een gewoon bestandspad.
    """
    onderdelen = spec.replace('\\', '/').split('!/')
    pad = onderdelen[0].lstrip('./')
    data = (PROJECT_ROOT / pad).read_bytes()
    for entry in onderdelen[1:]:
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            data = zf.read(entry)
    stream = io.BytesIO(data)
    stream.seek(0)
    return stream

# -----------------------------------------------------------------------------
# CKAN-interactie
# -----------------------------------------------------------------------------

def connect_ckan() -> RemoteCKAN:
    """
    Legt verbinding met de CKAN-server en verifieert de API-sleutel.
    Bij fouten wordt het script beëindigd.
    """
    logger.info("Verbinding maken met CKAN: %s", CKAN_URL)
    try:
        client = RemoteCKAN(CKAN_URL, apikey=API_KEY)
        client.action.status_show()
        logger.info("Authenticatie geslaagd")
        return client
    except Exception as e:
        logger.error("Kon niet verbinden of authenticeren: %s", e)
        sys.exit(1)


def ensure_organization(ckan: RemoteCKAN, org_name: str) -> str:
    """
    Controleert of een organisatie bestaat; maakt deze anders aan.
    Retourneert de slug (ID) van de organisatie.
    """
    slug = slugify(org_name)
    try:
        ckan.action.organization_show(id=slug)
        logger.info("Organisatie gevonden: %s", slug)
    except NotFound:
        org = ckan.action.organization_create(name=slug, title=org_name)
        slug = org['id']
        logger.info("Organisatie aangemaakt: %s", slug)
    return slug


def ensure_dataset(
    ckan: RemoteCKAN,
    title: str,
    description: str,
    org_id: str,
    resource_formats: Optional[List[str]] = None,
    resource_count: Optional[int] = None
) -> str:
    """
    Creëert of werkt een dataset bij met de volgende metadata:
      - titel en omschrijving
      - eigenaarorganisatie
      - optioneel resource-formaten en aantallen als extras
    """
    slug = slugify(title)
    params: Dict[str, Any] = {
        'name': slug,
        'title': title,
        'notes': description,
        'owner_org': org_id,
        'type': 'dataset'
    }
    extras: List[Dict[str,str]] = []
    if resource_formats:
        extras.append({ 'key': 'resource_formats', 'value': ','.join(sorted(set(resource_formats))) })
    if resource_count is not None:
        extras.append({ 'key': 'resource_count', 'value': str(resource_count) })
    if extras:
        params['extras'] = extras

    try:
        existing = ckan.action.package_show(id=slug)
        params['id'] = existing['id']
        logger.info("Dataset bijwerken: %s", slug)
        pkg = ckan.action.package_update(**params)
    except NotFound:
        logger.info("Dataset aanmaken: %s", slug)
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
    """
    Uploadt of vervangt een resource in een bestaande dataset.
    - Controle op naam met slugify
    - Bij update: behoud dezelfde resource-ID
    - Logt de URL van het resultaat
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
        search = ckan.action.resource_search(filters={'package_id': ds_id, 'name': rslug})
        if search.get('results'):
            payload['id'] = search['results'][0]['id']
            result = ckan.action.resource_update(**payload)
            logger.info("Resource bijgewerkt: %s → %s", filename, result.get('url',''))
            return
    except Exception:
        pass
    result = ckan.action.resource_create(**payload)
    logger.info("Resource aangemaakt: %s → %s", filename, result.get('url',''))

# -----------------------------------------------------------------------------
# Hoofdprogramma
# -----------------------------------------------------------------------------

def main():
    logger.info("=== Start: upload_resources.py ===")

    # Controleer of manifest beschikbaar is
    if not MANIFEST.exists():
        logger.error("Manifest niet gevonden: %s", MANIFEST)
        sys.exit(1)

    ckan = connect_ckan()

    # Optioneel: volledig opruimen van organisaties en datasets
    if input("Alle organisaties en datasets verwijderen? (ja/nee): ").lower().startswith('j'):
        for org in ckan.action.organization_list():
            for pkg in ckan.action.package_list(owner_org=org):
                ckan.action.package_delete(id=pkg)
                ckan.action.dataset_purge(id=pkg)
            ckan.action.organization_delete(id=org)
            ckan.action.organization_purge(id=org)
        logger.info("Alle organisaties en datasets zijn verwijderd.")

    # Bepaal upload-structuur: per package of per resource
    per_resource = input("Aparte dataset per resource? (ja/nee): ").lower().startswith('j')
    data = json.loads(MANIFEST.read_text())

    # Verwerk elke package
    for pkg_name, resources in data.items():
        logger.info("-- Pakket verwerken: %s", pkg_name)
        org_id = ensure_organization(ckan, pkg_name)

        if not per_resource:
            # Eén dataset voor alle resources
            formats = [r.get('format','') for r in resources]
            ds_id = ensure_dataset(
                ckan, pkg_name, description='', org_id=org_id,
                resource_formats=formats, resource_count=len(resources)
            )
            logger.info("Upload van %d resources naar dataset '%s' gestart", len(resources), pkg_name)
            for res in tqdm(resources, desc="Resources"):
                name = Path(res['upload']).name
                stream = open_stream(res['upload'])
                upload_resource(ckan, ds_id, name, stream, res.get('format',''), res.get('mimetype',''))
        else:
            # Eén dataset per resource
            for res in resources:
                name = Path(res['upload']).name
                fmt = res.get('format','')
                ds_id = ensure_dataset(
                    ckan, name, description=res.get('description',''), org_id=org_id,
                    resource_formats=[fmt], resource_count=1
                )
                logger.info("Upload resource '%s' naar eigen dataset", name)
                stream = open_stream(res['upload'])
                upload_resource(ckan, ds_id, name, stream, fmt, res.get('mimetype',''))

        logger.info("-- Pakket voltooid: %s --", pkg_name)

if __name__ == '__main__':
    main()

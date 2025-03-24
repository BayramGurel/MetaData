import os
import logging
from typing import List, Dict, Any, Optional
import PyPDF2
from openpyxl import load_workbook
import geopandas
import chardet
from pathlib import Path
import utils
import zipfile

log = logging.getLogger(__name__)


def _extract_metadata(file_path: Path) -> Optional[Dict[str, Any]]:
    """Extracts metadata based on file type."""
    file_ext = file_path.suffix.lower()
    metadata = {
        "bestandspad": str(file_path),
        "bestandsnaam": file_path.name,
        "bestandstype": None,
        "metadata": {},
        "relative_path": None,  # Will be set later
    }

    try:
        if file_ext == ".pdf":
            metadata["bestandstype"] = "PDF"
            metadata["metadata"] = _extract_pdf_metadata(file_path)
        elif file_ext in (".xlsx", ".xls"):
            metadata["bestandstype"] = "Excel"
            metadata["metadata"] = _extract_excel_metadata(file_path)
        elif file_ext == ".shp":
            metadata["bestandstype"] = "Shapefile"
            metadata["metadata"] = _extract_shapefile_metadata(file_path)
        elif file_ext == ".zip":  # .zip is handled in scan_en_extraheer
            return None
        else:
            log.warning(f"Unsupported file type: {file_path}")
            return None

        return metadata
    except Exception as e:
        log.error(f"Error extracting metadata from {file_path}: {e}", exc_info=True)
        return None


def _extract_pdf_metadata(file_path: Path) -> Dict[str, Any]:
    """Extracts metadata from a PDF."""
    metadata: Dict[str, Any] = {}
    try:
        with open(file_path, "rb") as f:
            pdf_reader = PyPDF2.PdfReader(f)
            doc_info = pdf_reader.metadata
            metadata["titel"] = doc_info.get("/Title")
            metadata["auteur"] = doc_info.get("/Author")
            metadata["onderwerp"] = doc_info.get("/Subject")
            metadata["creatiedatum"] = doc_info.get("/CreationDate")
            metadata["wijzigingsdatum"] = doc_info.get("/ModDate")
            metadata["producent"] = doc_info.get("/Producer")
            metadata["aantal_paginas"] = len(pdf_reader.pages)

            if xmp := doc_info.get("/Metadata"):
                try:
                    encoding = chardet.detect(xmp.get_data())['encoding'] or 'utf-8'
                    metadata["xmp"] = xmp.get_data().decode(encoding, errors='replace')
                except Exception as e:
                    log.warning(f"Error decoding XMP metadata in {file_path}: {e}")

    except Exception as e:
        log.error(f"Error extracting PDF metadata from {file_path}: {e}", exc_info=True)
    return metadata


def _extract_excel_metadata(file_path: Path) -> Dict[str, Any]:
    """Extracts metadata from an Excel file."""
    metadata: Dict[str, Any] = {}
    try:
        workbook = load_workbook(filename=file_path, read_only=True)
        metadata["werkbladen"] = workbook.sheetnames
        metadata["aantal_werkbladen"] = len(workbook.sheetnames)
        props = workbook.properties
        metadata["titel"] = props.title
        metadata["auteur"] = props.creator
        metadata["laatst_gewijzigd_door"] = props.lastModifiedBy
        metadata["creatiedatum"] = props.created
        metadata["wijzigingsdatum"] = props.modified
        metadata["onderwerp"] = props.subject
        metadata["categorie"] = props.category
        metadata["tags"] = props.keywords
    except Exception as e:
        log.error(f"Error extracting Excel metadata from {file_path}: {e}", exc_info=True)
    return metadata


def _extract_shapefile_metadata(file_path: Path) -> Dict[str, Any]:
    """Extracts metadata from a Shapefile."""
    metadata: Dict[str, Any] = {}
    try:
        gdf = geopandas.read_file(file_path)
        metadata["crs"] = str(gdf.crs)
        metadata["bounds"] = gdf.total_bounds.tolist()
        metadata["aantal_features"] = len(gdf)
        metadata["kolomnamen"] = gdf.columns.tolist()
        metadata["geometrie_types"] = gdf.geometry.type.unique().tolist()
    except Exception as e:
        log.error(f"Error extracting Shapefile metadata from {file_path}: {e}", exc_info=True)
    return metadata


def _process_directory(dir_path: Path, r_schijf_pad: str) -> Dict[str, Any]:
    """Processes a directory and returns its metadata."""
    log.debug(f"Processing directory: {dir_path}")
    return {
        "bestandspad": str(dir_path),
        "bestandsnaam": dir_path.name,
        "bestandstype": "directory",
        "metadata": {
            "last_modified": os.path.getmtime(dir_path),
        },
        "relative_path": str(dir_path.relative_to(r_schijf_pad)),
    }


def _process_file(file_path: Path, r_schijf_pad: str, temp_dir: Path,
                  zip_base_name: Optional[str] = None) -> List[Dict[str, Any]]:
    """Processes a single file, extracting metadata and handling ZIP files."""
    log.debug(f"Processing file: {file_path}")

    if file_path.suffix.lower() == ".zip":
        return _process_zip_file(file_path, r_schijf_pad, temp_dir)

    metadata = _extract_metadata(file_path)
    if not metadata:
        return []

    metadata["relative_path"] = str(file_path.relative_to(r_schijf_pad))
    metadata["metadata"]["file_hash"] = utils.calculate_file_hash(str(file_path))
    if zip_base_name:  # If processed from within a zip, add org_id
        metadata["metadata"]["potential_organization_id"] = zip_base_name
    return [metadata]


def _process_zip_file(zip_path: Path, r_schijf_pad: str, temp_dir: Path) -> List[Dict[str, Any]]:
    """Processes a ZIP file, extracting contents and processing each file."""
    log.debug(f"Extracting zip file: {zip_path}")
    zip_extract_path = temp_dir / zip_path.stem
    zip_extract_path.mkdir(parents=True, exist_ok=True)

    try:
        _extract_zip_recursively(zip_path, zip_extract_path)
    except Exception as e:
        log.error(f"Error extracting zip {zip_path}: {e}")
        return []  # Return empty list on extraction failure

    zip_base_name = zip_path.stem.lower().replace(" ", "-")  # Sanitize
    extracted_metadata = []

    for sub_root, _, sub_files in os.walk(zip_extract_path):
        for sub_file_name in sub_files:
            sub_file_path = Path(sub_root) / sub_file_name
            relative_path_in_zip = str(sub_file_path.relative_to(zip_extract_path))
            # Combine zip path with internal path for relative_path
            combined_relative_path = str(zip_path.relative_to(r_schijf_pad)) + "/" + relative_path_in_zip

            for metadata in _process_file(sub_file_path, r_schijf_pad, temp_dir, zip_base_name):
                metadata["relative_path"] = combined_relative_path  # Correct relative path
                extracted_metadata.append(metadata)

    return extracted_metadata


def _extract_zip_recursively(zip_path: Path, extract_to: Path):
    """Recursively extracts ZIP files, including nested ZIP files."""
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)

    for item in extract_to.glob("**/*.zip"):  # Find any .zip files recursively
        if item.is_file():
            nested_temp_dir = extract_to / item.stem  # Create subfolder
            nested_temp_dir.mkdir(parents=True, exist_ok=True)  # Ensure directory exists
            _extract_zip_recursively(item, nested_temp_dir)  # Recursive call
            item.unlink()  # Delete after extraction.



def scan_en_extraheer(r_schijf_pad: str) -> List[Dict[str, Any]]:
    """Scans the R-drive, extracts files (including from ZIPs), and extracts metadata."""
    metadata_lijst: List[Dict[str, Any]] = []
    temp_dir = Path("./temp_extraction")
    temp_dir.mkdir(parents=True, exist_ok=True)  # Ensure temp dir exists

    try:
        for root, dirs, files in os.walk(r_schijf_pad):
            for dir_name in dirs:
                dir_path = Path(root) / dir_name
                metadata_lijst.append(_process_directory(dir_path, r_schijf_pad))

            for file_name in files:
                file_path = Path(root) / file_name
                metadata_lijst.extend(_process_file(file_path, r_schijf_pad, temp_dir))

    finally:  # Ensure cleanup even if errors occur
        import shutil
        if temp_dir.exists():
            shutil.rmtree(temp_dir)  # Remove the entire temp directory

    log.debug(f"Total metadata entries: {len(metadata_lijst)}")
    return metadata_lijst


def valideer_metadata(metadata: Dict[str, Any]) -> Dict[str, str]:
    """Validates metadata."""
    validatie_resultaten: Dict[str, str] = {}
    bestandstype = metadata.get("bestandstype")

    if bestandstype == "PDF":
        if not utils.extract_value(metadata, "titel", "metadata"):
            validatie_resultaten["titel"] = "Ontbreekt"
        if (aantal_paginas := utils.extract_value(metadata, "aantal_paginas", "metadata")) and aantal_paginas > 1000:
            validatie_resultaten["aantal_paginas"] = "Te veel pagina's"

    elif bestandstype == "Excel":
        if (aantal_werkbladen := utils.extract_value(metadata, "aantal_werkbladen", "metadata")) and aantal_werkbladen > 25:
            validatie_resultaten["aantal_werkbladen"] = "Te veel werkbladen"
        if not utils.extract_value(metadata, "titel", "metadata"):
            validatie_resultaten["titel"] = "Ontbreekt"

    elif bestandstype == "Shapefile":
        if not utils.extract_value(metadata, "crs", "metadata"):
            validatie_resultaten["crs"] = "Ontbreekt"
        if not utils.extract_value(metadata, "geometrie_types", "metadata"):
            validatie_resultaten["geometrie_types"] = "Geen geometrie types"

    return validatie_resultaten


def transformeer_naar_ckan(metadata: Dict[str, Any], ckan_mapping: Dict[str, str]) -> Dict[str, Any]:
    """Transforms metadata to CKAN format."""
    ckan_metadata: Dict[str, Any] = {}

    for r_veld, ckan_veld in ckan_mapping.items():
        # Prefer values from the "metadata" sub-dictionary, but fall back
        waarde = utils.extract_value(metadata, r_veld, "metadata") or metadata.get(r_veld)
        if waarde is not None:
            if ckan_veld == "keywords" and isinstance(waarde, str):
                ckan_metadata[ckan_veld] = waarde.split(",")  # Split
            elif ckan_veld == "format":
                ckan_metadata[ckan_veld] = waarde.upper()  # Uppercase
            elif ckan_veld == "spatial_bbox" and isinstance(waarde, list):
                # GeoJSON conversion (if needed)
                ckan_metadata["spatial"] = {
                    "type": "Polygon",
                    "coordinates": [[[waarde[0], waarde[1]], [waarde[2], waarde[1]],
                                     [waarde[2], waarde[3]], [waarde[0], waarde[3]],
                                     [waarde[0], waarde[1]]]]
                }
            else:
                ckan_metadata[ckan_veld] = waarde  # Direct mapping

    return ckan_metadata
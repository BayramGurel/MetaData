# extract_transform.py
import os
import logging
import abc
from typing import List, Dict, Any, Optional, Type, Union, Tuple
from pathlib import Path
import PyPDF2
from openpyxl import load_workbook
try:
    import geopandas
    GEOPANDAS_AVAILABLE = True
except ImportError:
    GEOPANDAS_AVAILABLE = False
import chardet
import zipfile
import shutil
import datetime
import json
import re

# --- Dependency Import ---
try:
    from utils import calculate_file_hash
except ImportError:
    logging.warning("utils.calculate_file_hash not found. Using dummy hash.")
    calculate_file_hash = lambda filepath, **kwargs: f"dummy_hash_for_{Path(filepath).name}" # type: ignore

log = logging.getLogger(__name__)

# --- Constants ---
DEFAULT_TEMP_DIR = Path("./temp_extraction").resolve()

# --- File Handler Mapping ---
SUPPORTED_FILE_EXTENSIONS: Dict[str, Type['FileSystemItem']] = {} # Populated later

# --- Helper ---
def _safe_isoformat(dt: Optional[Any]) -> Optional[str]:
    """Safely convert datetime object to ISO UTC format string."""
    if isinstance(dt, datetime.datetime):
        if dt.tzinfo is None: dt = dt.replace(tzinfo=datetime.timezone.utc)
        return dt.isoformat()
    return None

def _parse_pdf_date_simple(date_str: Optional[Any]) -> Optional[str]:
    """Very basic PDF date string parser -> ISO format (assumes UTC)."""
    if not isinstance(date_str, str) or not date_str.startswith("D:") or len(date_str) < 9:
        return str(date_str) if date_str is not None else None
    core_dt_str = date_str[2:16] # Try YYYYMMDDHHMMSS
    try: return datetime.datetime.strptime(core_dt_str, '%Y%m%d%H%M%S').replace(tzinfo=datetime.timezone.utc).isoformat()
    except ValueError: pass # Ignore format errors
    try: return datetime.datetime.strptime(core_dt_str[:8], '%Y%m%d').replace(tzinfo=datetime.timezone.utc).isoformat() # Try YYYYMMDD
    except ValueError: pass
    log.debug(f"Could not parse PDF date '{date_str}'. Storing raw.")
    return date_str # Return original if parsing fails


# --- Base Class ---
class FileSystemItem(abc.ABC):
    """Abstract base class for filesystem items."""

    def __init__(self, path: Path, r_schijf_pad: Path, relative_path_override: Optional[str] = None):
        self.path = path.resolve()
        self.r_schijf_pad = r_schijf_pad.resolve()
        self.name = self.path.name
        self._relative_path_override = relative_path_override
        self._metadata_cache: Optional[Dict[str, Any]] = None
        if not self.path.exists(): log.warning(f"Path DNE on init: {self.path}")

    @property
    @abc.abstractmethod
    def item_type(self) -> str: pass

    @property
    def relative_path(self) -> str:
         if self._relative_path_override is not None: return self._relative_path_override
         try: return str(self.path.relative_to(self.r_schijf_pad))
         except ValueError: return str(self.path) # Fallback if not relative
         except Exception: return str(self.path) # Fallback on other errors

    @abc.abstractmethod
    def _extract_specific_metadata(self) -> Dict[str, Any]: pass

    def get_metadata(self) -> Optional[Dict[str, Any]]:
        """Gets metadata. Returns None only if path DNE or unreadable. Errors stored internally."""
        if self._metadata_cache is not None: return self._metadata_cache

        log.debug(f"Extracting ({self.item_type}): {self.path}")
        metadata: Dict[str, Any] = {
            "bestandspad": str(self.path), "bestandsnaam": self.name,
            "bestandstype": self.item_type, "relative_path": self.relative_path,
            "metadata": {}, "_extraction_timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }
        specific_meta: Dict[str, Any] = {}
        error_msg: Optional[str] = None

        try:
            # --- Core Checks ---
            if not self.path.exists(): raise FileNotFoundError(f"Path vanished: {self.path}")
            if not os.access(self.path, os.R_OK): raise PermissionError(f"No read permission: {self.path}")

            # --- Basic Stats (Common) ---
            try:
                 stat = self.path.stat()
                 specific_meta["file_size"] = stat.st_size
                 specific_meta["last_modified"] = _safe_isoformat(datetime.datetime.fromtimestamp(stat.st_mtime, datetime.timezone.utc))
            except Exception as stat_e: log.warning(f"Stat failed for {self.path}: {stat_e}") # Non-critical

            # --- Specific Extraction (Subclass) ---
            specific_meta.update(self._extract_specific_metadata())
            # Check if subclass reported internal error
            if err := specific_meta.pop("_extraction_error", None): error_msg = str(err)

            # --- Hash (Files only) ---
            if self.path.is_file():
                 specific_meta["file_hash"] = calculate_file_hash(self.path) # Returns None on failure
                 if specific_meta["file_hash"] is None: log.warning(f"Hash failed for: {self.path}")

        except FileNotFoundError as e: log.error(str(e)); return None # Critical: Cannot proceed
        except PermissionError as e: log.error(str(e)); error_msg = "Permission denied" # Record error, return partial meta
        except Exception as e: log.exception(f"Unexpected error getting metadata for {self.path}: {e}"); error_msg = f"Unexpected error: {e}"

        # --- Assemble & Cache ---
        metadata["metadata"] = specific_meta
        if error_msg: metadata["metadata"]["_extraction_error"] = error_msg # Store final error status
        self._metadata_cache = metadata
        return self._metadata_cache

# --- Concrete Subclasses --- (Focus on directness, error reporting)

class DirectoryItem(FileSystemItem):
    @property
    def item_type(self) -> str: return "directory"
    def _extract_specific_metadata(self) -> Dict[str, Any]: return {} # Base stats sufficient


class PdfFile(FileSystemItem):
    @property
    def item_type(self) -> str: return "PDF"
    def _extract_specific_metadata(self) -> Dict[str, Any]:
        meta: Dict[str, Any] = {}
        try:
            with self.path.open("rb") as f:
                reader = PyPDF2.PdfReader(f, strict=False)
                if doc_info := reader.metadata:
                    meta.update({
                        "titel": str(doc_info.get('/Title','')).strip() or None, # Use get with default, strip, store None if empty
                        "auteur": str(doc_info.get('/Author','')).strip() or None,
                        "onderwerp": str(doc_info.get('/Subject','')).strip() or None,
                        "producent": str(doc_info.get('/Producer','')).strip() or None,
                        "creatiedatum": _parse_pdf_date_simple(doc_info.get("/CreationDate")),
                        "wijzigingsdatum": _parse_pdf_date_simple(doc_info.get("/ModDate")),
                    })
                    # XMP
                    try:
                        if md_ref := doc_info.get('/Metadata'):
                            xmp_obj = reader.get_object(md_ref)
                            if xmp_obj and hasattr(xmp_obj, 'get_data'):
                                data = xmp_obj.get_data()
                                try: meta["xmp"] = data.decode('utf-8')
                                except UnicodeDecodeError:
                                    try: enc = chardet.detect(data)['encoding'] or 'utf-8'; meta["xmp"] = data.decode(enc, errors='replace')
                                    except Exception: meta["xmp"] = None
                    except Exception as x_e: log.warning(f"XMP error {self.path}: {x_e}")

                try: meta["aantal_paginas"] = len(reader.pages) if not reader.is_encrypted else None
                except Exception: meta["aantal_paginas"] = None
        except PyPDF2.errors.PdfReadError as e: meta["_extraction_error"] = f"PyPDF2 Read Error: {e}"
        except Exception as e: meta["_extraction_error"] = f"PDF Error: {e}"; log.exception("PDF Error")
        return meta


class ExcelFile(FileSystemItem):
    @property
    def item_type(self) -> str: return "Excel"
    def _extract_specific_metadata(self) -> Dict[str, Any]:
        meta: Dict[str, Any] = {}
        wb = None
        try:
            wb = load_workbook(filename=self.path, read_only=True, data_only=True, keep_links=False)
            meta["werkbladen"] = wb.sheetnames or []
            meta["aantal_werkbladen"] = len(meta["werkbladen"])
            if props := wb.properties:
                meta.update({
                    "titel": props.title, "auteur": props.creator, "onderwerp": props.subject,
                    "tags": props.keywords, "categorie": props.category,
                    "laatst_gewijzigd_door": props.lastModifiedBy,
                    "creatiedatum": _safe_isoformat(props.created),
                    "wijzigingsdatum": _safe_isoformat(props.modified)
                })
        except zipfile.BadZipFile: meta["_extraction_error"] = "Invalid XLSX format"
        except Exception as e: meta["_extraction_error"] = f"Excel Error: {e}"; log.exception("Excel Error")
        finally:
            if wb: wb.close()
        return meta


class ShapefileFile(FileSystemItem):
    @property
    def item_type(self) -> str: return "Shapefile"
    def _extract_specific_metadata(self) -> Dict[str, Any]:
        if not GEOPANDAS_AVAILABLE: return {"_extraction_error": "Geopandas not installed"}
        meta: Dict[str, Any] = {}
        try:
            gdf = geopandas.read_file(self.path)
            meta.update({
                "crs": str(gdf.crs) if gdf.crs else None,
                "bounds": gdf.total_bounds.tolist(),
                "aantal_features": len(gdf),
                "kolomnamen": gdf.columns.tolist(),
                "geometrie_types": gdf.geometry.type.unique().tolist() if gdf.geometry is not None and not gdf.geometry.empty else []
            })
        except ImportError: meta["_extraction_error"] = "Geopandas import failed"
        except Exception as e: meta["_extraction_error"] = f"Shapefile Error: {e}"; log.exception("Shapefile Error")
        return meta


class ZipFileItem(FileSystemItem):
    @property
    def item_type(self) -> str: return "ZIP"
    def _extract_specific_metadata(self) -> Dict[str, Any]:
        meta: Dict[str, Any] = {}
        try:
             with zipfile.ZipFile(self.path, 'r') as zf:
                 if zf.comment:
                      try: meta["comment"] = zf.comment.decode('utf-8', errors='replace')
                      except Exception as e: log.warning(f"Zip comment decode error: {e}")
        except zipfile.BadZipFile: meta["_extraction_error"] = "Invalid ZIP format"
        except Exception as e: meta["_extraction_error"] = f"ZIP Meta Error: {e}"; log.exception("ZIP Meta Error")
        return meta

# --- Handler Mapping ---
SUPPORTED_FILE_EXTENSIONS = {
    ".pdf": PdfFile, ".xlsx": ExcelFile, ".xls": ExcelFile, ".shp": ShapefileFile,
}

# --- Metadata Scanner (Simplified Sequential Implementation) ---
class MetadataScanner:
    """Scans directories sequentially, extracts metadata, handles ZIPs recursively."""

    def __init__(self, root_scan_path: Union[str, Path], temp_dir: Union[str, Path] = DEFAULT_TEMP_DIR):
        self.r_schijf_pad = Path(root_scan_path).resolve()
        self.temp_dir_base = Path(temp_dir).resolve()
        self.current_run_temp_dir: Optional[Path] = None # Unique temp dir per scan()
        if not self.r_schijf_pad.is_dir(): raise ValueError(f"Invalid root path: {self.r_schijf_pad}")
        log.info(f"Scanner Initialized. Root: {self.r_schijf_pad}, Temp Base: {self.temp_dir_base}")

    def _create_item(self, path: Path, relative_override: Optional[str] = None) -> Optional[FileSystemItem]:
        """Factory method to create FileSystemItem."""
        try:
            p_resolved = path.resolve()
            if not p_resolved.exists(): return None # Skip non-existent quietly now
        except OSError as e: log.error(f"OS error checking path '{path}': {e}"); return None

        cls: Optional[Type[FileSystemItem]] = None
        if p_resolved.is_dir(): cls = DirectoryItem
        elif p_resolved.is_file():
            ext = p_resolved.suffix.lower()
            if ext == ".zip": cls = ZipFileItem
            else: cls = SUPPORTED_FILE_EXTENSIONS.get(ext)

        if cls:
             try: return cls(p_resolved, self.r_schijf_pad, relative_override)
             except Exception as e: log.exception(f"Init {cls.__name__} failed for {p_resolved}: {e}")
        # else: Unsupported type or not file/dir
        return None

    def _extract_zip_recursive(self, zip_path: Path, extract_dir: Path, processed: set) -> bool:
        """Extracts ZIP recursively. Returns True on success, False on critical failure."""
        abs_zip_path = zip_path.resolve()
        if abs_zip_path in processed: return True # Skip loops
        processed.add(abs_zip_path)
        log.debug(f"Extracting {zip_path} -> {extract_dir}")
        try:
            extract_dir.mkdir(parents=True, exist_ok=True)
            with zipfile.ZipFile(zip_path, 'r') as zf:
                for member in zf.infolist():
                    try:
                        target = (extract_dir / member.filename).resolve()
                        # Basic path traversal check
                        if not str(target).startswith(str(extract_dir.resolve())):
                             log.warning(f"Skipping unsafe member: {member.filename} in {zip_path}")
                             continue
                        zf.extract(member, path=extract_dir)
                    except Exception as e: log.error(f"Err extracting member {member.filename}: {e}") # Continue
            # Recurse
            all_nested_ok = True
            for item in extract_dir.rglob("*.zip"):
                 if item.is_file():
                      nested_dir = extract_dir / f"{item.stem}_nested_{abs_zip_path.name}"
                      if not self._extract_zip_recursive(item, nested_dir, processed.copy()):
                           all_nested_ok = False # Propagate failure up if needed
                      try: item.unlink(missing_ok=True) # Cleanup nested zip
                      except OSError as e: log.warning(f"Failed nested zip cleanup {item}: {e}")
            return all_nested_ok
        except (zipfile.BadZipFile, FileNotFoundError, PermissionError, OSError) as e:
            log.error(f"Extraction failed for {zip_path}: {e}")
            return False # Critical failure for this zip
        except Exception as e:
            log.exception(f"Unexpected error extracting {zip_path}: {e}")
            return False

    def _process_extracted_item(self, item_path: Path, base_extract_dir: Path,
                                zip_item: ZipFileItem) -> Optional[Dict[str, Any]]:
        """Processes a single file found inside an extracted zip."""
        try:
            rel_in_zip = str(item_path.relative_to(base_extract_dir))
            combined_rel = f"{zip_item.relative_path}/{rel_in_zip}"
            fs_item = self._create_item(item_path, relative_override=combined_rel)
            if not fs_item: return None

            metadata = fs_item.get_metadata()
            if metadata:
                # Add context
                metadata["metadata"]["_source_zip_file"] = str(zip_item.path)
                metadata["metadata"]["potential_organization_id"] = zip_item.path.stem.lower().replace(" ", "-").replace("_", "-")
                return metadata
            else:
                log.warning(f"Metadata failed for extracted item: {item_path}")
                return None
        except ValueError as e: log.error(f"Path error processing extracted item {item_path}: {e}")
        except Exception as e: log.exception(f"Error processing extracted item {item_path}: {e}")
        return None

    def _process_zip_sequentially(self, zip_item: ZipFileItem) -> List[Dict[str, Any]]:
        """Extracts zip and processes contents sequentially."""
        if not self.current_run_temp_dir: return []
        abs_zip_path = zip_item.path.resolve()
        extract_dir = self.current_run_temp_dir / f"{zip_item.path.stem}_{abs_zip_path.name}_contents"
        log.info(f"Processing contents of zip: {zip_item.path}")
        all_zip_meta = []
        try:
            if not self._extract_zip_recursive(zip_item.path, extract_dir, set()):
                log.error(f"Aborting content processing due to extraction failure: {zip_item.path}")
                return [] # Stop if extraction failed

            # Sequentially process extracted files
            for root, _, files in os.walk(extract_dir):
                 for name in files:
                      file_path = Path(root) / name
                      meta = self._process_extracted_item(file_path, extract_dir, zip_item)
                      if meta: all_zip_meta.append(meta)
        except Exception as e:
            log.exception(f"Error during sequential processing of zip contents {zip_item.path}: {e}")
        finally:
            # Cleanup this zip's specific temp dir
            if extract_dir.exists(): shutil.rmtree(extract_dir, ignore_errors=True)
        return all_zip_meta

    def scan(self) -> List[Dict[str, Any]]:
        """Performs a sequential scan, extracts metadata, handles ZIPs recursively."""
        all_metadata: List[Dict[str, Any]] = []
        processed_paths: set = set()

        log.info(f"Starting sequential scan: {self.r_schijf_pad}")
        ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d_%H%M%S")
        self.current_run_temp_dir = self.temp_dir_base / f"scan_{ts}"

        try:
            self.current_run_temp_dir.mkdir(parents=True, exist_ok=True)
            log.info(f"Using run temp dir: {self.current_run_temp_dir}")

            for root, dirs, files in os.walk(self.r_schijf_pad, topdown=True, onerror=log.error):
                 root_path = Path(root)
                 for name in files + dirs: # Process all items found
                      item_path = root_path / name
                      try:
                          abs_path = item_path.resolve()
                          if abs_path in processed_paths: continue
                          processed_paths.add(abs_path) # Mark processed now

                          item = self._create_item(item_path)
                          if not item: continue

                          if isinstance(item, ZipFileItem):
                               # 1. Get metadata for the ZIP file itself
                               zip_meta = item.get_metadata()
                               if zip_meta: all_metadata.append(zip_meta)
                               # 2. Extract & process contents sequentially
                               content_meta = self._process_zip_sequentially(item)
                               all_metadata.extend(content_meta)
                               # Note: We don't add internal paths to processed_paths to keep it simple.
                          else:
                               # Process regular file or directory
                               item_meta = item.get_metadata()
                               if item_meta: all_metadata.append(item_meta)

                      except OSError as e: log.error(f"OS error processing {item_path}: {e}")
                      except Exception as e: log.exception(f"Unexpected error on {item_path}: {e}")

        finally:
            # Ensure main temp dir for the run is cleaned up
            if self.current_run_temp_dir and self.current_run_temp_dir.exists():
                log.info(f"Cleaning up run temp dir: {self.current_run_temp_dir}")
                shutil.rmtree(self.current_run_temp_dir, ignore_errors=True)
            self.current_run_temp_dir = None

        log.info(f"Sequential scan complete. Found {len(all_metadata)} metadata entries.")
        return all_metadata


# --- Validation & Transformation Classes --- (Keep structure, refine logic)

class MetadataValidator:
    """Validates extracted metadata."""
    # Keep previous implementation, seems reasonable and separate
    def validate(self, metadata: Dict[str, Any]) -> Dict[str, str]:
        results: Dict[str, str] = {}
        item_type = metadata.get("bestandstype")
        spec_meta = metadata.get("metadata", {})
        path = metadata.get('bestandspad', '?')
        if err := spec_meta.get("_extraction_error"): results["_extraction_error"] = f"Extraction error: {err}"
        validators = {"PDF": self._v_pdf, "Excel": self._v_excel, "Shapefile": self._v_shp}
        if func := validators.get(item_type): results.update(func(spec_meta))
        if results: log.debug(f"Validation issues for {path}: {results}")
        return results
    def _v_pdf(self, m: Dict[str, Any]) -> Dict[str, str]:
        e = {}; p = m.get("aantal_paginas")
        if not m.get("titel"): e["titel"] = "PDF Title missing"
        if p is None: e["aantal_paginas"] = "Page count N/A"
        elif isinstance(p,int) and p > 1000: e["aantal_paginas"] = f"Too many pages ({p}>1000)"
        return e
    def _v_excel(self, m: Dict[str, Any]) -> Dict[str, str]:
        e = {}; s = m.get("aantal_werkbladen")
        if not m.get("titel"): e["titel"] = "Excel Title missing"
        if s is None: e["aantal_werkbladen"] = "Sheet count N/A"
        elif isinstance(s,int) and s > 50: e["aantal_werkbladen"] = f"Too many sheets ({s}>50)"
        return e
    def _v_shp(self, m: Dict[str, Any]) -> Dict[str, str]:
        e = {}
        if not m.get("crs"): e["crs"] = "CRS missing"
        if not m.get("geometrie_types"): e["geometrie_types"] = "Geom types missing"
        return e


class CkanTransformer:
    """Transforms metadata for CKAN."""
    # Keep previous implementation, seems reasonable and separate
    def __init__(self, ckan_mapping: Dict[str, str]): self.map = ckan_mapping
    def _slugify(self, t: str, ml: int = 100) -> str:
        if not t: return "unnamed"; s = str(t).lower().strip(); s = re.sub(r'[\s\._]+', '-', s); s = re.sub(r'[^\w-]', '', s); s = re.sub(r'-{2,}', '-', s); s = s.strip('-')[:ml]; return s or "unnamed"
    def transform(self, metadata: Dict[str, Any]) -> Dict[str, Any]:
        out: Dict[str, Any] = {}; spec = metadata.get("metadata", {}); p = metadata.get('bestandspad','?')
        for src, target in self.map.items():
            v = spec.get(src, metadata.get(src))
            if v is None: continue
            try:
                if target == "tags" and isinstance(v, str): out[target] = [t.strip() for t in v.split(',') if t.strip()]
                elif target == "format" and isinstance(v, str): out[target] = v.upper()
                elif target == "spatial_bbox" and isinstance(v, list) and len(v) == 4:
                    coords = [float(c) for c in v]; out["spatial"] = json.dumps({"type": "Polygon","coordinates": [[[coords[0], coords[1]], [coords[2], coords[1]], [coords[2], coords[3]], [coords[0], coords[3]], [coords[0], coords[1]]]]})
                else: out[target] = v
            except Exception as e: log.warning(f"Transform {src}->{target} error for {p}: {e}")
        if not out.get('title'): out['title'] = metadata.get('bestandsnaam', 'Untitled')
        if not out.get('name'): out['name'] = self._slugify(out['title'])
        log.debug(f"Transformed metadata for {p}")
        return out


# --- Example Usage --- (Simplified setup)
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)-8s %(name)s: %(message)s')
    TEST_DIR = Path("./_test_scan_data_simple")
    try:
        log.info(f"Setting up test dir {TEST_DIR}")
        TEST_DIR.mkdir(exist_ok=True)
        (TEST_DIR / "doc1.pdf").write_text("pdf")
        (TEST_DIR / "sheet1.xlsx").touch()
        zip_path = TEST_DIR / "data.zip"
        with zipfile.ZipFile(zip_path, 'w') as zf: zf.writestr("inside.txt", "zip content")
        log.info("Test files created.")

        scanner = MetadataScanner(TEST_DIR) # Default temp dir
        validator = MetadataValidator()
        transformer = CkanTransformer({"bestandstype": "format", "titel": "title", "name": "name", "file_hash": "hash"}) # Minimal map

        log.info("--- Starting Scan ---")
        all_meta = scanner.scan()
        log.info(f"--- Scan Finished ({len(all_meta)} items) ---")

        for meta in all_meta:
            print("-" * 20)
            path = meta.get('relative_path')
            errors = validator.validate(meta)
            transformed = transformer.transform(meta)
            print(f"PATH: {path}")
            print(f"VALIDATION: {'OK' if not errors else errors}")
            print(f"TRANSFORMED: {json.dumps(transformed, indent=2)}")

    except Exception as e: log.exception(f"Error in example: {e}")
    finally:
        if TEST_DIR.exists(): log.info(f"Cleaning up {TEST_DIR}"); shutil.rmtree(TEST_DIR, ignore_errors=True)
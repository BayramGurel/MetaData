import configparser
import logging
import os
import shutil
import sys
import time
import zipfile
from pathlib import Path
from typing import List, Tuple, Optional, Dict

# Probeer ckanapi te importeren
try:
    from ckanapi import RemoteCKAN, NotAuthorized, NotFound, ValidationError
except ImportError:
    print("Fout: 'ckanapi' module niet gevonden.")
    print("Installeer deze met: pip install ckanapi")
    sys.exit(1)

# Globale logger instantie
logger = logging.getLogger(__name__)

# --- Configuratie Klasse (Aangepast) ---
class ConfigLoader:
    """Laadt en beheert de pipeline configuratie uit een .ini bestand."""

    def __init__(self, config_path: Path):
        self.config_path = config_path
        self.config = configparser.ConfigParser()
        self.script_dir = Path(__file__).parent

        if not config_path.is_file():
            logger.critical(f"Configuratiebestand '{config_path}' niet gevonden!")
            raise FileNotFoundError(f"Configuratiebestand '{config_path}' niet gevonden.")

        try:
            self.config.read(config_path)
            self._load_settings()
            self._validate_settings()
            self._show_security_warning()
        except (configparser.Error, ValueError, KeyError) as e:
            logger.critical(f"Fout bij lezen/verwerken config file '{config_path}': {e}")
            raise ValueError(f"Fout bij lezen/verwerken config file '{config_path}': {e}") from e

    def _load_settings(self):
        """Interne methode om settings te laden naar attributen."""
        # Paden
        self.source_dir = Path(self.config.get('Paths', 'source_dir'))
        self.staging_dir = Path(self.config.get('Paths', 'staging_dir'))
        processed_dir_str = self.config.get('Paths', 'processed_dir', fallback=None)
        log_file_rel = self.config.get('Paths', 'log_file')
        self.log_file = (self.script_dir / log_file_rel).resolve()

        # CKAN
        self.ckan_url = self.config.get('CKAN', 'ckan_url')
        self.ckan_api_key = self.config.get('CKAN', 'api_key')
        # self.ckan_owner_org = self.config.get('CKAN', 'owner_org') # Niet meer nodig voor plaatsing

        # Pipeline
        self.move_processed = self.config.getboolean('Pipeline', 'move_processed_files', fallback=False)
        self.processed_dir = Path(processed_dir_str) if self.move_processed and processed_dir_str else None

        # Zip Handling
        extensions_str = self.config.get('ZipHandling', 'relevant_extensions', fallback='')
        self.relevant_extensions = [ext.strip().lower() for ext in extensions_str.split(',') if ext.strip() and ext.strip().startswith('.')]
        self.extract_nested_zips = self.config.getboolean('ZipHandling', 'extract_nested_zips', fallback=False)

        # Behaviour (Nieuw)
        self.create_organizations = self.config.getboolean('Behaviour', 'create_organizations', fallback=False)


    def _validate_settings(self):
        """Controleert essentiële settings."""
        if not self.source_dir.is_dir():
            logger.warning(f"Bronmap '{self.source_dir}' bestaat niet of is geen map.")
        if not self.ckan_url or not self.ckan_api_key:
             # owner_org is niet meer verplicht hier
            raise ValueError("CKAN URL en API Key moeten ingesteld zijn in config.ini")

    def _show_security_warning(self):
        """Toont de security waarschuwing m.b.t. de API key."""
        if self.ckan_api_key == 'JOUW_CKAN_API_KEY_HIER':
            logger.warning("CKAN API Key is NIET ingesteld in config.ini (gebruikt placeholder).")
        elif len(self.ckan_api_key) < 20:
            logger.warning("De ingestelde CKAN API key lijkt erg kort. Controleer config.ini.")
        logger.warning("=" * 30)
        logger.warning("WAARSCHUWING: Hardcodeer API keys NOOIT in scripts voor productie of versiebeheer.")
        logger.warning("Gebruik environment variables of een secrets manager.")
        if self.create_organizations:
             logger.warning("WAARSCHUWING: 'create_organizations' staat op True. Dit vereist sysadmin rechten voor de API key!")
        logger.warning("=" * 30)

# --- Logging Setup Functie (Onveranderd) ---
def setup_logging(log_file_path: Path):
    # ... (zelfde als voorheen) ...
    try:
        log_file_path.parent.mkdir(parents=True, exist_ok=True)
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - [%(name)s:%(lineno)d] - %(message)s',
            handlers=[
                logging.FileHandler(log_file_path, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ],
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        logging.getLogger('ckanapi').setLevel(logging.WARNING)
        logger.info(f"Logging geïnitialiseerd. Logbestand: {log_file_path}")
    except Exception as e:
        print(f"Kritieke fout bij opzetten logging naar {log_file_path}: {e}")
        sys.exit(1)


# --- CKAN Handler Klasse (Aangepast) ---
class CkanHandler:
    """Verantwoordelijk voor alle interacties met de CKAN API."""

    def __init__(self, ckan_url: str, api_key: str):
        # Geen default_owner_org meer nodig
        self.ckan_url = ckan_url
        self.api_key = api_key
        try:
            self.ckan = RemoteCKAN(self.ckan_url, apikey=self.api_key)
            logger.info(f"CKAN Handler geïnitialiseerd voor URL: {self.ckan_url}")
        except Exception as e:
            logger.critical(f"Kon geen verbinding maken met CKAN op {self.ckan_url}: {e}")
            raise ConnectionError(f"CKAN connectie mislukt: {e}") from e

    def check_organization_exists(self, org_id_or_name: str) -> Dict:
        # ... (Deze methode blijft hetzelfde als in de vorige versie) ...
        logger.debug(f"Controleren organisatie: '{org_id_or_name}'")
        try:
            org_details = self.ckan.call_action('organization_show', {'id': org_id_or_name, 'include_datasets': False})
            logger.info(f"Organisatie '{org_id_or_name}' (ID: {org_details['id']}) gevonden.")
            return org_details
        except NotFound:
            logger.info(f"Organisatie '{org_id_or_name}' niet gevonden in CKAN.") # Info level, want we kunnen proberen te maken
            raise FileNotFoundError(f"Organisatie '{org_id_or_name}' niet gevonden.")
        except NotAuthorized as e:
            logger.error(f"Permissiefout: Geen toegang tot organisatie '{org_id_or_name}'. API key rechten? Fout: {e}")
            raise PermissionError(f"Geen toegang tot organisatie '{org_id_or_name}'.") from e
        except Exception as e:
            logger.error(f"Fout bij controleren organisatie '{org_id_or_name}': {e}")
            raise ConnectionError(f"Fout bij controleren organisatie '{org_id_or_name}'.") from e


    # *** NIEUWE METHODE: create_organization ***
    def create_organization(self, org_id: str, org_title: str) -> Dict:
        """Probeert een nieuwe organisatie aan te maken."""
        logger.info(f"Poging tot aanmaken nieuwe organisatie ID='{org_id}', Titel='{org_title}'...")
        try:
            # Voeg eventueel meer metadata toe aan de organisatie
            org_data = {
                'name': org_id,       # De URL-vriendelijke ID
                'title': org_title,   # De leesbare titel
                'description': f'Organisatie automatisch aangemaakt door pipeline op {time.strftime("%Y-%m-%d")}',
                # 'image_url': '...', # Optioneel
            }
            created_org = self.ckan.call_action('organization_create', org_data)
            logger.info(f"Organisatie '{org_id}' (ID: {created_org['id']}) succesvol aangemaakt.")
            return created_org
        except NotAuthorized as e:
            # Dit gebeurt als de API key geen sysadmin rechten heeft
            logger.error(f"Permissiefout: Niet geautoriseerd om organisatie '{org_id}' aan te maken. Vereist sysadmin rechten! Fout: {e}")
            raise PermissionError(f"Aanmaken organisatie '{org_id}' niet toegestaan (sysadmin nodig?).") from e
        except ValidationError as e:
            # Bv. als de naam al bestaat of ongeldige tekens bevat
            error_details = e.error_dict if hasattr(e, 'error_dict') else str(e)
            logger.error(f"CKAN Validatiefout bij aanmaken organisatie '{org_id}': {error_details}")
            raise ValueError(f"Organisatie validatiefout: {error_details}") from e
        except Exception as e:
            logger.error(f"Fout bij aanmaken CKAN organisatie '{org_id}': {e}")
            raise ConnectionError(f"Aanmaken organisatie '{org_id}' mislukt: {e}") from e


    def get_or_create_dataset(self, dataset_id: str, dataset_title: str, owner_org_id: str, zip_name: str) -> Dict:
        # ... (Deze methode blijft hetzelfde, gebruikt nu de doorgegeven owner_org_id) ...
        logger.debug(f"Dataset '{dataset_id}' zoeken of aanmaken in org ID '{owner_org_id}'...")
        try:
            package = self.ckan.call_action('package_show', {'id': dataset_id})
            logger.info(f"Dataset '{dataset_id}' (ID: {package['id']}) bestaat al.")
            try:
                update_notes = package.get('notes', '').split('\nLaatst bijgewerkt:')[0]
                update_notes += f'\nLaatst bijgewerkt met inhoud van {zip_name} op {time.strftime("%Y-%m-%d %H:%M:%S")}'
                self.ckan.call_action('package_patch', {'id': package['id'], 'notes': update_notes})
                logger.debug(f"Dataset '{dataset_id}' metadata bijgewerkt.")
            except Exception as patch_err:
                 logger.warning(f"Kon metadata van dataset '{dataset_id}' niet bijwerken: {patch_err}")
            return package
        except NotFound:
            logger.info(f"Dataset '{dataset_id}' bestaat niet. Poging tot aanmaken...")
            try:
                package_data = {
                    'name': dataset_id,
                    'title': dataset_title,
                    'owner_org': owner_org_id, # Gebruik de correcte org ID
                    'notes': f'Dataset automatisch aangemaakt voor ZIP {zip_name} op {time.strftime("%Y-%m-%d %H:%M:%S")}',
                    'author': 'Data Pipeline Script OOP',
                }
                package = self.ckan.call_action('package_create', package_data)
                logger.info(f"Dataset '{dataset_id}' (ID: {package['id']}) succesvol aangemaakt.")
                return package
            except NotAuthorized as e:
                 logger.error(f"Permissiefout: Niet geautoriseerd om dataset '{dataset_id}' aan te maken in org ID '{owner_org_id}'. Fout: {e}")
                 raise PermissionError(f"Dataset aanmaken in org ID '{owner_org_id}' niet toegestaan.") from e
            except ValidationError as e:
                 error_details = e.error_dict if hasattr(e, 'error_dict') else str(e)
                 logger.error(f"CKAN Validatiefout bij aanmaken dataset '{dataset_id}': {error_details}")
                 raise ValueError(f"Dataset validatiefout: {error_details}") from e
            except Exception as e:
                logger.error(f"Fout bij aanmaken CKAN dataset '{dataset_id}': {e}")
                raise ConnectionError(f"Dataset aanmaken mislukt: {e}") from e
        except NotAuthorized as e:
             logger.error(f"Permissiefout: Niet geautoriseerd om dataset '{dataset_id}' te bekijken. Fout: {e}")
             raise PermissionError(f"Bekijken dataset '{dataset_id}' niet toegestaan.") from e
        except Exception as e:
            logger.error(f"Algemene fout bij zoeken/aanmaken CKAN dataset '{dataset_id}': {e}")
            raise ConnectionError(f"Zoeken/aanmaken dataset mislukt: {e}") from e

    def get_existing_resources(self, package_id: str) -> Dict[str, str]:
        # ... (Onveranderd) ...
        logger.debug(f"Ophalen bestaande resources voor package ID '{package_id}'...")
        existing_resources = {}
        try:
            package_details = self.ckan.call_action('package_show', {'id': package_id, 'include_resources': True})
            for res in package_details.get('resources', []):
                if res.get('name'):
                     existing_resources[res['name']] = res['id']
            logger.debug(f"{len(existing_resources)} bestaande resource(s) met naam gevonden.")
            return existing_resources
        except Exception as e:
            logger.warning(f"Kon bestaande resources voor package ID '{package_id}' niet ophalen: {e}. Update check minder efficiënt.")
            return {}

    def upload_resource(self, package_id: str, file_path: Path, resource_name: str, description: str, format: str, existing_resource_id: Optional[str] = None):
        # ... (Onveranderd) ...
        action = 'resource_update' if existing_resource_id else 'resource_create'
        log_prefix = "Updaten" if existing_resource_id else "Aanmaken"
        logger.info(f"{log_prefix} resource '{resource_name}' voor package ID '{package_id}'...")
        try:
            with open(file_path, 'rb') as file_object:
                resource_data = {
                    'package_id': package_id,
                    'name': resource_name,
                    'description': description,
                    'format': format,
                    'upload': file_object,
                }
                if existing_resource_id:
                    resource_data['id'] = existing_resource_id
                result = self.ckan.call_action(action, resource_data)
                logger.info(f"Resource '{resource_name}' (ID: {result['id']}) succesvol {log_prefix.lower()}d.")
        except FileNotFoundError:
            logger.error(f"Bestand '{file_path}' niet gevonden tijdens upload poging.")
            raise
        except ValidationError as e:
            error_details = e.error_dict if hasattr(e, 'error_dict') else str(e)
            logger.error(f"CKAN Validatiefout voor resource '{resource_name}': {error_details}")
            raise
        except NotAuthorized as e:
             logger.error(f"Permissiefout: Niet geautoriseerd om resource '{resource_name}' te {action}n. Fout: {e}")
             raise
        except Exception as e:
            logger.error(f"Algemene fout bij {action} resource '{resource_name}': {e}")
            raise

# --- ZIP Processor Klasse (Onveranderd) ---
class ZipProcessor:
    # ... (Deze klasse blijft exact hetzelfde als in de vorige OOP versie) ...
    """Verantwoordelijk voor het verwerken van één enkel ZIP bestand."""

    def __init__(self, source_zip_path: Path, config: ConfigLoader):
        self.source_path = source_zip_path
        self.config = config
        self.staged_zip_path: Optional[Path] = None
        self.extract_subdir: Optional[Path] = None
        self.files_to_upload: List[Path] = []
        self.original_name = source_zip_path.name

        timestamp = int(time.time())
        unique_suffix = f"{self.source_path.stem}_{timestamp}"
        self.staged_zip_path_potential = self.config.staging_dir / f"{unique_suffix}{self.source_path.suffix}"
        self.extract_subdir_potential = self.config.staging_dir / f"extracted_{unique_suffix}"

    def stage(self) -> Path:
        logger.info(f"[{self.original_name}] Kopiëren naar staging...")
        self.config.staging_dir.mkdir(parents=True, exist_ok=True)
        try:
            self.staged_zip_path = self.staged_zip_path_potential
            shutil.copy2(self.source_path, self.staged_zip_path)
            logger.info(f"[{self.original_name}] Gekopieerd naar '{self.staged_zip_path.relative_to(self.config.staging_dir)}'")
            return self.staged_zip_path
        except Exception as e:
            logger.error(f"[{self.original_name}] Fout bij kopiëren naar staging: {e}")
            raise IOError(f"Fout bij kopiëren naar staging: {e}") from e

    def extract(self) -> Tuple[Path, List[Path]]:
        if not self.staged_zip_path or not self.staged_zip_path.is_file():
             raise FileNotFoundError(f"[{self.original_name}] Gestaged ZIP bestand niet gevonden voor extractie.")

        logger.info(f"[{self.original_name}] Start uitpakken naar '{self.extract_subdir_potential.relative_to(self.config.staging_dir)}'")
        self.extract_subdir = self.extract_subdir_potential
        extracted_files_all = []
        try:
            self.extract_subdir.mkdir(parents=True, exist_ok=True)
            with zipfile.ZipFile(self.staged_zip_path, 'r') as zip_ref:
                corrupt_files = zip_ref.testzip()
                if corrupt_files:
                    logger.warning(f"[{self.original_name}] ZIP bevat mogelijk corrupte bestanden: {corrupt_files}")
                zip_ref.extractall(self.extract_subdir)
                for item in self.extract_subdir.rglob('*'):
                    if item.is_file():
                        extracted_files_all.append(item)
            logger.info(f"[{self.original_name}] Uitpakken voltooid. {len(extracted_files_all)} bestanden gevonden.")

            if self.config.relevant_extensions:
                self.files_to_upload = [f for f in extracted_files_all if f.suffix.lower() in self.config.relevant_extensions]
                logger.info(f"[{self.original_name}] {len(self.files_to_upload)} bestanden geselecteerd op basis van extensies.")
            else:
                self.files_to_upload = extracted_files_all
                logger.info(f"[{self.original_name}] Alle {len(self.files_to_upload)} uitgepakte bestanden worden meegenomen (geen filter).")

            if not self.config.extract_nested_zips:
                 original_count = len(self.files_to_upload)
                 self.files_to_upload = [f for f in self.files_to_upload if f.suffix.lower() != '.zip']
                 if len(self.files_to_upload) < original_count:
                      logger.debug(f"[{self.original_name}] {original_count - len(self.files_to_upload)} .zip bestanden uitgesloten van upload.")

            if self.config.extract_nested_zips:
                 logger.warning(f"[{self.original_name}] Extractie van geneste ZIPs is ingeschakeld maar niet geïmplementeerd.")

            return self.extract_subdir, self.files_to_upload

        except zipfile.BadZipFile:
            logger.error(f"[{self.original_name}] Corrupt of ongeldig ZIP bestand.")
            self._safe_remove_dir(self.extract_subdir)
            raise ValueError(f"Corrupt ZIP: {self.original_name}") from None
        except Exception as e:
            logger.error(f"[{self.original_name}] Fout bij uitpakken: {e}")
            self._safe_remove_dir(self.extract_subdir)
            raise IOError(f"Fout bij uitpakken: {e}") from e

    def cleanup_staging(self):
        logger.info(f"[{self.original_name}] Opruimen staging area...")
        if self.staged_zip_path and self.staged_zip_path.exists():
            try:
                self.staged_zip_path.unlink()
                logger.debug(f"[{self.original_name}] Gestaged ZIP '{self.staged_zip_path.name}' verwijderd.")
            except OSError as e:
                logger.warning(f"[{self.original_name}] Kon gestaged ZIP '{self.staged_zip_path.name}' niet verwijderen: {e}")
        else:
             logger.debug(f"[{self.original_name}] Geen gestaged ZIP gevonden om te verwijderen.")
        self._safe_remove_dir(self.extract_subdir)

    def _safe_remove_dir(self, dir_to_remove: Optional[Path]):
         if dir_to_remove and dir_to_remove.exists() and dir_to_remove.is_dir():
            if self.config.staging_dir in dir_to_remove.parents:
                try:
                    shutil.rmtree(dir_to_remove)
                    logger.debug(f"[{self.original_name}] Extractie map '{dir_to_remove.name}' verwijderd.")
                except OSError as e:
                    logger.warning(f"[{self.original_name}] Kon extractie map '{dir_to_remove.name}' niet verwijderen: {e}")
            else:
                logger.error(f"[{self.original_name}] WEIGEREN te verwijderen: Map '{dir_to_remove}' valt buiten staging dir '{self.config.staging_dir}'.")
         elif dir_to_remove:
              logger.debug(f"[{self.original_name}] Extractie map '{dir_to_remove.name}' niet gevonden of geen map.")

    def move_to_processed(self):
        if not self.config.processed_dir:
            logger.warning(f"[{self.original_name}] Kan niet naar 'processed' verplaatsen: map niet geconfigureerd.")
            return
        logger.info(f"[{self.original_name}] Verplaatsen origineel bestand naar: {self.config.processed_dir}")
        try:
            self.config.processed_dir.mkdir(parents=True, exist_ok=True)
            dest_path = self.config.processed_dir / self.original_name
            counter = 1
            original_dest_path = dest_path
            while dest_path.exists():
                 timestamp = int(time.time())
                 dest_path = self.config.processed_dir / f"{self.source_path.stem}_{timestamp}_{counter}{self.source_path.suffix}"
                 counter += 1
            if dest_path != original_dest_path:
                 logger.warning(f"[{self.original_name}] Bestond al in processed dir. Hernoemd naar '{dest_path.name}'.")
            shutil.move(str(self.source_path), str(dest_path))
            logger.info(f"[{self.original_name}] Origineel bestand succesvol verplaatst naar '{dest_path}'.")
        except Exception as e:
            logger.error(f"[{self.original_name}] Fout bij verplaatsen naar processed dir: {e}. Bestand blijft in '{self.config.source_dir}'.")


# --- Pipeline Klasse (Aangepast) ---
class Pipeline:
    """Orkestreert het volledige data pipeline proces."""

    def __init__(self, config: ConfigLoader):
        self.config = config
        # Initialiseer CkanHandler zonder default org
        self.ckan_handler = CkanHandler(config.ckan_url, config.ckan_api_key)
        self.total_processed_zips = 0
        self.total_error_zips = 0
        logger.info("Pipeline geïnitialiseerd.")
        logger.info(f"Automatisch organisaties aanmaken: {self.config.create_organizations}")

    def _detect_input_files(self) -> List[Path]:
        # ... (Onveranderd) ...
        logger.info(f"Zoeken naar input ZIPs in: {self.config.source_dir}")
        if not self.config.source_dir.is_dir():
             logger.error(f"Bronmap {self.config.source_dir} niet gevonden!")
             return []
        try:
            files = [f for f in self.config.source_dir.iterdir() if f.is_file() and f.suffix.lower() == '.zip']
            logger.info(f"{len(files)} input ZIP(s) gevonden.")
            return files
        except Exception as e:
             logger.error(f"Fout bij lezen van bronmap {self.config.source_dir}: {e}")
             return []

    def _process_single_zip(self, zip_path: Path, index: int, total_files: int):
        # ... (Logica voor staging, extract, cleanup, move blijft hetzelfde) ...
        zip_processor = ZipProcessor(zip_path, self.config)
        zip_start_time = time.time()
        logger.info(f"--- [{index}/{total_files}] Start verwerking ZIP: '{zip_processor.original_name}' ---")
        zip_processed_successfully = False

        try:
            zip_processor.stage()
            extract_subdir, files_to_upload = zip_processor.extract()

            # Aanroep naar aangepaste _publish_to_ckan
            self._publish_to_ckan(
                zip_processor.original_name,
                extract_subdir,
                files_to_upload
            )

            zip_processed_successfully = True
            self.total_processed_zips += 1
            logger.info(f"[{zip_processor.original_name}] Succesvol verwerkt.")

        except (FileNotFoundError, IOError, ValueError, ConnectionError, PermissionError, Exception) as e:
            self.total_error_zips += 1
            logger.error(f"[{zip_processor.original_name}] FOUT tijdens verwerking: {e}", exc_info=False)
            logger.debug(f"[{zip_processor.original_name}] Traceback:", exc_info=True)

        finally:
            zip_processor.cleanup_staging()
            if self.config.move_processed and zip_processor.config.processed_dir:
                 if zip_processed_successfully:
                      zip_processor.move_to_processed()
                 else:
                      logger.warning(f"[{zip_processor.original_name}] Wordt NIET verplaatst naar processed dir vanwege fouten.")

            zip_duration = time.time() - zip_start_time
            status = "VOLTOOID" if zip_processed_successfully else "MISLUKT"
            logger.info(f"--- [{index}/{total_files}] Einde verwerking ZIP: '{zip_processor.original_name}'. Status: {status}. Duur: {zip_duration:.2f} sec. ---")


    # *** AANGEPASTE _publish_to_ckan METHODE ***
    def _publish_to_ckan(self, original_zip_name: str, extract_subdir: Path, files_to_upload: List[Path]):
        """Bepaalt organisatie, checkt/maakt aan, en publiceert bestanden."""
        logger.info(f"[{original_zip_name}] Start CKAN publicatie...")

        if not files_to_upload:
             logger.warning(f"[{original_zip_name}] Geen bestanden om naar CKAN te publiceren.")
             return

        # Stap 1: Leid organisatie en dataset namen af van ZIP naam
        base_name = Path(original_zip_name).stem
        # ID's: lowercase, alphanumeric/hyphen
        target_org_id = ''.join(c if c.isalnum() else '-' for c in base_name).lower().strip('-')
        dataset_id = target_org_id # Vaak is de dataset gerelateerd aan de 'organisatie' van de data
        # Titels: leesbaarder
        target_org_title = base_name.replace('_', ' ').replace('-', ' ').title()
        dataset_title = target_org_title # Titel ook hetzelfde voor nu

        # Voorkom lege ID's
        if not target_org_id:
             timestamp_id = f"org-or-dataset-{int(time.time())}"
             logger.warning(f"[{original_zip_name}] Kon geen geldige org/dataset ID afleiden, gebruik: '{timestamp_id}'")
             target_org_id = timestamp_id
             dataset_id = timestamp_id
             if not target_org_title: target_org_title = f"Organisatie {timestamp_id}"
             if not dataset_title: dataset_title = f"Dataset {timestamp_id}"


        logger.info(f"[{original_zip_name}] Doel Organisatie ID: '{target_org_id}', Titel: '{target_org_title}'")
        logger.info(f"[{original_zip_name}] Doel Dataset ID: '{dataset_id}', Titel: '{dataset_title}'")

        organization = None
        owner_org_id = None # ID van de gevonden of aangemaakte organisatie

        try:
            # Stap 2: Zoek of maak de organisatie aan
            try:
                # Probeer organisatie te vinden
                organization = self.ckan_handler.check_organization_exists(target_org_id)
                owner_org_id = organization['id']
            except FileNotFoundError:
                # Organisatie niet gevonden, probeer aan te maken indien geconfigureerd
                logger.info(f"[{original_zip_name}] Organisatie '{target_org_id}' niet gevonden.")
                if self.config.create_organizations:
                    logger.warning(f"[{original_zip_name}] Poging tot automatisch aanmaken organisatie '{target_org_id}' (vereist sysadmin)...")
                    try:
                        organization = self.ckan_handler.create_organization(target_org_id, target_org_title)
                        owner_org_id = organization['id']
                    except PermissionError as pe:
                        # Specifiek falen door gebrek aan sysadmin rechten
                        logger.error(f"[{original_zip_name}] Kan organisatie '{target_org_id}' niet aanmaken: {pe}")
                        raise # Gooi opnieuw op om deze ZIP te stoppen
                    except (ValueError, ConnectionError) as ce:
                         # Andere fouten bij aanmaken (validatie, connectie)
                         logger.error(f"[{original_zip_name}] Kan organisatie '{target_org_id}' niet aanmaken: {ce}")
                         raise # Gooi opnieuw op om deze ZIP te stoppen
                else:
                    # Niet gevonden en automatisch aanmaken staat uit
                    logger.error(f"[{original_zip_name}] Organisatie '{target_org_id}' niet gevonden en 'create_organizations' staat uit. Kan niet doorgaan.")
                    raise FileNotFoundError(f"Doel organisatie '{target_org_id}' niet gevonden en aanmaken is uitgeschakeld.")
            except PermissionError as e:
                 # Geen toegang tot bestaande organisatie
                 logger.error(f"[{original_zip_name}] Geen toegang tot bestaande organisatie '{target_org_id}': {e}")
                 raise # Gooi opnieuw op
            except ConnectionError as e:
                 # Andere fout bij checken
                 logger.error(f"[{original_zip_name}] Fout bij controleren organisatie '{target_org_id}': {e}")
                 raise # Gooi opnieuw op

            # Als we hier zijn, hebben we een geldige owner_org_id
            if not owner_org_id:
                 # Veiligheidscheck, zou niet mogen gebeuren
                 raise ValueError(f"[{original_zip_name}] Kon geen geldige organisatie ID bepalen voor '{target_org_id}'.")

            logger.info(f"[{original_zip_name}] Gebruiken Organisatie ID: '{owner_org_id}'")

            # Stap 3: Get or Create Dataset binnen de gevonden/gemaakte organisatie
            package = self.ckan_handler.get_or_create_dataset(dataset_id, dataset_title, owner_org_id, original_zip_name)
            package_id = package['id']

            # Stap 4: Get Existing Resources
            existing_resources = self.ckan_handler.get_existing_resources(package_id)

            # Stap 5: Upload/Update Resources
            successful_resource_uploads = 0
            failed_resource_uploads = 0
            for file_path in files_to_upload:
                # ... (logica voor resource upload blijft hetzelfde) ...
                resource_name = file_path.name
                relative_path = file_path.relative_to(extract_subdir)
                description = f"Bestand '{relative_path}' uit {original_zip_name}, verwerkt op {time.strftime('%Y-%m-%d %H:%M:%S')}"
                format = file_path.suffix.lstrip('.').upper() if file_path.suffix else 'Unknown'
                existing_resource_id = existing_resources.get(resource_name)

                try:
                    self.ckan_handler.upload_resource(
                        package_id, file_path, resource_name, description, format, existing_resource_id
                    )
                    successful_resource_uploads += 1
                except Exception as resource_error:
                    logger.error(f"[{original_zip_name}] Kon resource '{resource_name}' niet verwerken: {resource_error}")
                    failed_resource_uploads += 1

            logger.info(f"[{original_zip_name}] CKAN resource verwerking voltooid. Succes: {successful_resource_uploads}, Fouten: {failed_resource_uploads}.")
            if failed_resource_uploads > 0:
                raise ConnectionError(f"{failed_resource_uploads} resource(s) mislukt voor ZIP '{original_zip_name}'.")

        except (FileNotFoundError, ValueError, PermissionError, ConnectionError) as e:
             logger.error(f"[{original_zip_name}] Fout bij CKAN organisatie/dataset niveau: {e}")
             raise # Gooi opnieuw op zodat _process_single_zip faalt
        except Exception as e:
             logger.error(f"[{original_zip_name}] Onverwachte fout tijdens CKAN publicatie: {e}")
             raise


    def run(self):
        # ... (Deze methode blijft hetzelfde als in de vorige OOP versie) ...
        run_start_time = time.time()
        logger.info("="*50)
        logger.info("--- Start Data Pipeline Run (OOP Version) ---")
        logger.info(f"Configuratie geladen van: {self.config.config_path}")
        logger.info(f"Bron: '{self.config.source_dir}', Staging: '{self.config.staging_dir}'")
        logger.info(f"CKAN: '{self.config.ckan_url}'")
        # Log de nieuwe behaviour flag
        logger.info(f"Organisaties automatisch aanmaken: {self.config.create_organizations} (Vereist sysadmin!)")
        logger.info(f"Verplaats verwerkt: {self.config.move_processed}")
        logger.info("="*50)

        self.total_processed_zips = 0
        self.total_error_zips = 0
        input_files = []

        try:
            input_files = self._detect_input_files()

            if not input_files:
                logger.info("Geen input bestanden gevonden om te verwerken.")
            else:
                 logger.info(f"Start verwerking van {len(input_files)} ZIP bestand(en)...")
                 for i, file_path in enumerate(input_files, 1):
                      self._process_single_zip(file_path, i, len(input_files))

        except Exception as e:
            logger.critical(f"Onverwachte kritieke fout in pipeline run: {e}", exc_info=True)
            self.total_error_zips = len(input_files) - self.total_processed_zips

        finally:
            run_end_time = time.time()
            run_duration = run_end_time - run_start_time
            logger.info("="*50)
            logger.info("--- Einde Data Pipeline Run ---")
            logger.info(f"Totaal aantal ZIPs gevonden: {len(input_files)}")
            logger.info(f"Totaal succesvol verwerkte ZIPs: {self.total_processed_zips}")
            logger.info(f"Totaal aantal ZIPs met fouten: {self.total_error_zips}")
            logger.info(f"Totale duur van de run: {run_duration:.2f} seconden.")
            logger.info("="*50)


# --- Hoofd Execution Block (Onveranderd) ---
if __name__ == "__main__":
    CONFIG_FILE = Path(__file__).parent / 'config.ini'
    temp_log_path = Path(__file__).parent / 'logs' / 'pipeline_init.log'
    try:
        temp_log_path.parent.mkdir(parents=True, exist_ok=True)
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[logging.FileHandler(temp_log_path), logging.StreamHandler(sys.stdout)])
    except Exception as log_init_e:
         print(f"FATAL: Kon initiële logging niet starten: {log_init_e}")
         sys.exit(1)

    try:
        config = ConfigLoader(CONFIG_FILE)
        setup_logging(config.log_file) # Herconfigureer met juiste pad
        pipeline = Pipeline(config)
        pipeline.run()
    except FileNotFoundError as e:
        logger.critical(f"Kritieke fout: {e}. Pipeline stopt.")
        sys.exit(1)
    except (ValueError, ConnectionError, PermissionError) as e:
        logger.critical(f"Kritieke initialisatiefout: {e}. Pipeline stopt.", exc_info=True)
        sys.exit(1)
    except Exception as e:
        logger.critical(f"Onverwachte kritieke fout in hoofduitvoering: {e}", exc_info=True)
        sys.exit(1)

    logger.info("Script uitvoering voltooid.")
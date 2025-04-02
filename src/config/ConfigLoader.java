// Save as: src/com/yourcompany/config/ConfigLoader.java
package config;

import util.LoggingUtil;
import org.slf4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.CodeSource;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.Objects; // For null checks

/**
 * Loads, validates, and provides access to application configuration
 * from a properties file. Supports path resolution relative to the
 * application's execution directory and environment variable overrides
 * for sensitive data like API keys.
 */
public class ConfigLoader {

    // --- Constants for Property Keys ---
    private static final String KEY_SOURCE_DIR = "Paths.source_dir";
    private static final String KEY_STAGING_DIR = "Paths.staging_dir";
    private static final String KEY_PROCESSED_DIR = "Paths.processed_dir";
    private static final String KEY_LOG_FILE = "Paths.log_file";
    private static final String KEY_CKAN_URL = "CKAN.ckan_url";
    private static final String KEY_CKAN_API_KEY = "CKAN.api_key";
    private static final String KEY_MOVE_PROCESSED = "Pipeline.move_processed_files";
    private static final String KEY_RELEVANT_EXTENSIONS = "ZipHandling.relevant_extensions";
    private static final String KEY_EXTRACT_NESTED_ZIPS = "ZipHandling.extract_nested_zips";
    private static final String KEY_CREATE_ORGS = "Behaviour.create_organizations";

    private static final String ENV_CKAN_API_KEY = "CKAN_API_KEY"; // Environment variable for API Key

    private static final Logger logger = LoggingUtil.getLogger(ConfigLoader.class);

    private final Path configPath;
    private final Properties properties;
    private final Path executionDir; // Directory containing the JAR or classes

    // Configuration values (effectively final after constructor)
    private Path sourceDir;
    private Path stagingDir;
    private Path processedDir; // Optional
    private Path logFile;
    private String ckanUrl;
    private String ckanApiKey;
    private boolean apiKeyFromEnv; // Track source of API Key
    private boolean moveProcessed;
    private List<String> relevantExtensions;
    private boolean extractNestedZips;
    private boolean createOrganizations;

    /**
     * Loads and validates configuration from the specified properties file path.
     *
     * @param configPath Path to the configuration properties file.
     * @throws IOException If the file cannot be read or essential directories are invalid/inaccessible.
     * @throws IllegalArgumentException If the configuration contains missing required keys,
     * invalid values (paths, booleans), or inconsistencies.
     */
    public ConfigLoader(Path configPath) throws IOException, IllegalArgumentException {
        this.configPath = Objects.requireNonNull(configPath, "configPath cannot be null").toAbsolutePath();
        this.properties = new Properties();
        this.executionDir = determineExecutionDirectory();
        logger.info("Resolved execution directory: {}", executionDir);

        if (!Files.isReadable(this.configPath)) {
            String errorMsg = String.format("Configuration file '%s' not found or not readable!", this.configPath);
            // Logging might not be fully set up yet, use stderr as well
            System.err.println("ERROR: " + errorMsg);
            logger.error(errorMsg); // Log before throwing
            throw new IOException(errorMsg);
        }

        try (InputStream input = new FileInputStream(this.configPath.toFile())) {
            properties.load(input);
            logger.info("Configuration properties loaded from: {}", this.configPath);
            loadAndValidateSettings();
            showSecurityWarning();
            logLoadedConfiguration(); // Log summary of loaded config
        } catch (IOException e) {
            // Catch specific file read errors
            String errorMsg = String.format("Error reading configuration file '%s': %s", this.configPath, e.getMessage());
            logger.error(errorMsg, e);
            throw new IOException(errorMsg, e);
        } catch (IllegalArgumentException e) {
            // Catch validation errors from loadAndValidateSettings
            String errorMsg = String.format("Invalid configuration in '%s': %s", this.configPath, e.getMessage());
            logger.error(errorMsg, e);
            // Re-throw validation exceptions directly
            throw e;
        }
    }

    // Determines execution directory (JAR location or classes root), falls back to CWD.
    private Path determineExecutionDirectory() {
        try {
            CodeSource codeSource = ConfigLoader.class.getProtectionDomain().getCodeSource();
            if (codeSource != null && codeSource.getLocation() != null) {
                Path path = Paths.get(codeSource.getLocation().toURI()).toAbsolutePath();
                if (Files.isDirectory(path)) {
                    logger.debug("Determined execution path (directory): {}", path);
                    return path;
                } else if (Files.isRegularFile(path) && path.getFileName().toString().toLowerCase().endsWith(".jar")) {
                    logger.debug("Determined execution path (JAR file): {}", path);
                    // Return parent directory of the JAR file
                    return path.getParent();
                } else {
                    logger.warn("CodeSource location is neither a directory nor a JAR file: {}. Falling back to CWD.", path);
                }
            } else {
                logger.warn("Could not get CodeSource location. Falling back to CWD.");
            }
        } catch (URISyntaxException | SecurityException | NullPointerException | InvalidPathException e) {
            logger.warn("Could not reliably determine execution directory (Error: {}). Falling back to CWD.", e.getMessage());
        }
        // Fallback to current working directory
        Path cwd = Paths.get(".").toAbsolutePath();
        logger.info("Execution directory determined via fallback (CWD): {}", cwd);
        return cwd;
    }

    // Gets required property or throws IllegalArgumentException
    private String getRequiredProperty(String key) throws IllegalArgumentException {
        String value = properties.getProperty(key);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(String.format("Missing required configuration key: '%s'", key));
        }
        return value.trim();
    }

    // Resolves path relative to execution directory if not absolute
    private Path resolvePathProperty(String value) throws InvalidPathException {
        Path path = Paths.get(value);
        if (path.isAbsolute()) {
            logger.debug("Path '{}' is absolute.", path);
            return path.normalize(); // Normalize even absolute paths
        } else {
            // Resolve against execution directory
            Path resolvedPath = executionDir.resolve(value).normalize();
            logger.debug("Resolved relative path '{}' to '{}' based on execution directory '{}'.", value, resolvedPath, executionDir);
            return resolvedPath;
        }
    }

    // Gets required Path property, resolving relative paths
    private Path getRequiredPathProperty(String key) throws IllegalArgumentException {
        String pathStr = getRequiredProperty(key);
        try {
            return resolvePathProperty(pathStr);
        } catch (InvalidPathException e) {
            throw new IllegalArgumentException(String.format("Invalid path format for key '%s': '%s'", key, pathStr), e);
        }
    }

    // Gets optional Path property, resolving relative paths
    private Path getOptionalPathProperty(String key) throws IllegalArgumentException {
        String pathStr = properties.getProperty(key, "").trim();
        if (pathStr.isEmpty()) {
            return null;
        }
        try {
            return resolvePathProperty(pathStr);
        } catch (InvalidPathException e) {
            // Treat invalid optional paths as if they were not set, but log warning
            logger.warn("Invalid path format for optional key '{}': '{}'. Ignoring setting.", key, pathStr, e);
            return null;
            // Or optionally: throw new IllegalArgumentException(...) if invalid path for optional key is critical
        }
    }

    // Gets boolean property with default, parsing leniently
    private boolean getBooleanProperty(String key, boolean defaultValue) {
        String value = properties.getProperty(key);
        if (value == null) {
            return defaultValue;
        }
        String trimmedValue = value.trim().toLowerCase();
        if ("true".equals(trimmedValue) || "yes".equals(trimmedValue) || "1".equals(trimmedValue)) {
            return true;
        } else if ("false".equals(trimmedValue) || "no".equals(trimmedValue) || "0".equals(trimmedValue)) {
            return false;
        } else {
            logger.warn("Unrecognized boolean value for key '{}': '{}'. Using default: {}", key, value, defaultValue);
            return defaultValue;
        }
    }

    // Central method to load and validate all settings
    private void loadAndValidateSettings() throws IllegalArgumentException, IOException {
        // Paths
        sourceDir = getRequiredPathProperty(KEY_SOURCE_DIR);
        stagingDir = getRequiredPathProperty(KEY_STAGING_DIR);
        logFile = getRequiredPathProperty(KEY_LOG_FILE); // Log file path resolved here

        // CKAN - Prefer Environment Variable for API Key
        ckanUrl = getRequiredProperty(KEY_CKAN_URL);
        String apiKeyFromEnvVar = System.getenv(ENV_CKAN_API_KEY);
        if (apiKeyFromEnvVar != null && !apiKeyFromEnvVar.trim().isEmpty()) {
            ckanApiKey = apiKeyFromEnvVar.trim();
            apiKeyFromEnv = true;
            logger.info("Loaded CKAN API Key from environment variable '{}'", ENV_CKAN_API_KEY);
        } else {
            ckanApiKey = getRequiredProperty(KEY_CKAN_API_KEY); // Fallback to properties file
            apiKeyFromEnv = false;
            logger.info("Loaded CKAN API Key from configuration file key '{}'", KEY_CKAN_API_KEY);
        }

        // Pipeline
        moveProcessed = getBooleanProperty(KEY_MOVE_PROCESSED, false);
        processedDir = getOptionalPathProperty(KEY_PROCESSED_DIR); // Can return null
        if (moveProcessed && processedDir == null) {
            logger.warn("'{}' is true, but '{}' is not set or invalid in config file '{}'. Processed files will NOT be moved.",
                    KEY_MOVE_PROCESSED, KEY_PROCESSED_DIR, configPath);
            moveProcessed = false; // Disable if dir is invalid/missing
        }

        // Zip Handling
        String extensionsStr = properties.getProperty(KEY_RELEVANT_EXTENSIONS, "").trim();
        if (!extensionsStr.isEmpty()) {
            relevantExtensions = Arrays.stream(extensionsStr.split(","))
                    .map(String::trim)
                    .filter(ext -> !ext.isEmpty() && ext.startsWith("."))
                    .map(String::toLowerCase)
                    .distinct()
                    .collect(Collectors.toList());
        } else {
            relevantExtensions = Collections.emptyList();
        }
        extractNestedZips = getBooleanProperty(KEY_EXTRACT_NESTED_ZIPS, false);

        // Behaviour
        createOrganizations = getBooleanProperty(KEY_CREATE_ORGS, false);

        // --- Post-load Validations ---
        validateUrl(ckanUrl, KEY_CKAN_URL);
        validateDirectories();
    }

    // Helper for basic URL validation
    private void validateUrl(String url, String key) throws IllegalArgumentException {
        if (!url.toLowerCase().startsWith("http://") && !url.toLowerCase().startsWith("https://")) {
            // Allow just a domain? Maybe not for CKAN URL. Be strict.
            throw new IllegalArgumentException(
                    String.format("Invalid URL format for key '%s': '%s'. Must start with http:// or https://", key, url)
            );
        }
        // Could add more robust URL syntax validation if needed (e.g., using java.net.URL constructor)
    }

    // Helper to validate all required directories
    private void validateDirectories() throws IOException {
        // Check/Create essential directories
        validateDirectory("Source", sourceDir, true, false, true); // Check exists, readable, creatable
        validateDirectory("Staging", stagingDir, true, true, true); // Check exists, readable, writable, creatable

        if (moveProcessed && processedDir != null) {
            validateDirectory("Processed", processedDir, true, true, true); // Check exists, readable, writable, creatable
        }

        // Ensure log directory is creatable/writable
        Path logParentDir = (logFile != null) ? logFile.getParent() : null;
        if (logParentDir != null) {
            validateDirectory("Log Directory", logParentDir, true, true, true); // Ensure log dir exists and is writable
        } else if (logFile != null) {
            // Log file has no parent (e.g., specified relative to execution dir root)
            validateDirectory("Execution (for Logging)", executionDir, false, true, false); // Check execution dir writable
        } else {
            // Should not happen as logFile is required, but handle defensively
            throw new IllegalStateException("Log file path is null after loading configuration.");
        }
    }


    // Helper for basic directory validation and creation attempt
    private void validateDirectory(String dirPurpose, Path dirPath, boolean checkExists, boolean checkWritable, boolean attemptCreate) throws IOException {
        Objects.requireNonNull(dirPath, dirPurpose + " directory path cannot be null");
        String dirDesc = String.format("%s directory '%s'", dirPurpose, dirPath);

        try {
            if (checkExists && !Files.exists(dirPath)) {
                if (attemptCreate) {
                    logger.warn("{} does not exist. Attempting to create.", dirDesc);
                    try {
                        Files.createDirectories(dirPath);
                        logger.info("Successfully created {}", dirDesc);
                        // Check again after creation attempt (redundant check, createDirectories throws if failed)
                        // if (!Files.exists(dirPath)) { throw new IOException("Failed to create " + dirDesc + "."); }
                    } catch (IOException | SecurityException e) {
                        throw new IOException("Failed to create " + dirDesc + ": " + e.getMessage(), e);
                    }
                } else {
                    throw new IOException(dirDesc + " does not exist and creation was not attempted.");
                }
            }

            // If it exists (or was just created), perform further checks
            if (Files.exists(dirPath)) {
                if (!Files.isDirectory(dirPath)) {
                    throw new IOException(String.format("Path '%s' exists but is not a directory.", dirPath));
                }
                if (!Files.isReadable(dirPath)) {
                    throw new IOException(dirDesc + " is not readable.");
                }
                if (checkWritable && !Files.isWritable(dirPath)) {
                    throw new IOException(dirDesc + " is not writable.");
                }
                logger.debug("{} validation passed (Readable: {}, Writable Checked: {} -> {}).",
                        dirDesc, Files.isReadable(dirPath), checkWritable, checkWritable ? Files.isWritable(dirPath) : "N/A");
            } else if (!attemptCreate && checkExists) {
                // Should have been caught earlier if checkExists=true and attemptCreate=false
                throw new IllegalStateException(dirDesc + " does not exist, validation logic error.");
            }


        } catch (SecurityException e) {
            // Catch permission errors during checks
            throw new IOException("Permission error accessing " + dirDesc + ": " + e.getMessage(), e);
        }
        // Catch InvalidPathException earlier during path resolution
    }

    // Logs security warnings related to API key storage
    private void showSecurityWarning() {
        boolean keySeemsInsecure = false;
        if (!apiKeyFromEnv) {
            keySeemsInsecure = true;
            logger.warn("CKAN API Key was loaded directly from the configuration file: {}", configPath);
            if ("YOUR_CKAN_API_KEY_HERE".equalsIgnoreCase(ckanApiKey) || ckanApiKey == null || ckanApiKey.isEmpty()) {
                logger.error("CKAN API Key is NOT set correctly in {}. Pipeline WILL fail authorization.", configPath);
            } else if (ckanApiKey.length() < 20) { // Arbitrary short length check
                logger.warn("The CKAN API key configured in {} seems very short. Please verify it.", configPath);
            }
        } else {
            // Key is from environment variable - better, but still warn if placeholder/short
            if ("YOUR_CKAN_API_KEY_HERE".equalsIgnoreCase(ckanApiKey) || ckanApiKey == null || ckanApiKey.isEmpty()) {
                logger.error("CKAN API Key environment variable '{}' is NOT set correctly. Pipeline WILL fail authorization.", ENV_CKAN_API_KEY);
            } else if (ckanApiKey.length() < 20) {
                logger.warn("The CKAN API key from environment variable '{}' seems very short. Please verify it.", ENV_CKAN_API_KEY);
            }
        }

        if (keySeemsInsecure) {
            logger.warn("======================== SECURITY BEST PRACTICE ========================");
            logger.warn("Storing secrets like API keys directly in configuration files is discouraged.");
            logger.warn("RECOMMENDATION: Use environment variables ('{}'), secrets management tools", ENV_CKAN_API_KEY);
            logger.warn("(like HashiCorp Vault, AWS/GCP/Azure Secrets Manager), or other secure methods.");
            logger.warn("========================================================================");
        }

        if (createOrganizations) {
            logger.warn("!!! CONFIGURATION WARNING: '{}' is TRUE. This requires the CKAN API key to have SYSADMIN privileges !!!", KEY_CREATE_ORGS);
        }
    }

    // Logs a summary of the loaded (non-sensitive) configuration
    private void logLoadedConfiguration() {
        logger.info("--- Loaded Configuration Summary ---");
        logger.info("{}: {}", KEY_SOURCE_DIR, sourceDir);
        logger.info("{}: {}", KEY_STAGING_DIR, stagingDir);
        logger.info("{}: {}", KEY_PROCESSED_DIR, processedDir != null ? processedDir : "N/A");
        logger.info("{}: {}", KEY_LOG_FILE, logFile);
        logger.info("{}: {}", KEY_CKAN_URL, ckanUrl);
        logger.info("CKAN API Key Source: {}", apiKeyFromEnv ? "Environment Variable ("+ENV_CKAN_API_KEY+")" : "Config File ("+KEY_CKAN_API_KEY+")");
        logger.info("{}: {}", KEY_MOVE_PROCESSED, moveProcessed);
        logger.info("{}: {}", KEY_RELEVANT_EXTENSIONS, relevantExtensions);
        logger.info("{}: {}", KEY_EXTRACT_NESTED_ZIPS, extractNestedZips);
        logger.info("{}: {}", KEY_CREATE_ORGS, createOrganizations);
        logger.info("--- End Configuration Summary ---");
    }


    // --- Getters ---
    // Use Javadoc or annotations (@Nonnull/@Nullable) if strict null-safety is needed
    public Path getConfigPath() { return configPath; }
    public Path getSourceDir() { return sourceDir; }
    public Path getStagingDir() { return stagingDir; }
    /** @return The configured processed directory, or null if not configured or invalid. */
    public Path getProcessedDir() { return processedDir; }
    public Path getLogFile() { return logFile; }
    public String getCkanUrl() { return ckanUrl; }
    /** @implNote This returns the actual API key. Handle with care. */
    public String getCkanApiKey() { return ckanApiKey; }
    public boolean isMoveProcessed() { return moveProcessed; }
    /** @return An unmodifiable list of lower-case relevant file extensions (including leading dot). */
    public List<String> getRelevantExtensions() { return Collections.unmodifiableList(relevantExtensions); }
    public boolean isExtractNestedZips() { return extractNestedZips; }
    public boolean isCreateOrganizations() { return createOrganizations; }
    public Path getExecutionDir() { return executionDir; }
}
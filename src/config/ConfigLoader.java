// Save as: src/config/ConfigLoader.java
package config;

import util.LoggingUtil;
import org.slf4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI; // Use URI for validation
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
import java.util.Objects;

/**
 * Loads, validates, and provides access to application configuration
 * from a properties file. Supports path resolution relative to the
 * application's execution directory and environment variable overrides
 * for sensitive data like API keys.
 *
 * This class is final to prevent subclassing and ensure configuration loading behavior
 * is self-contained. Configuration values are effectively immutable after construction.
 */
public final class ConfigLoader { // Made class final

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

    // --- Configuration values (final, set during construction) ---
    private final Path sourceDir;
    private final Path stagingDir;
    private final Path processedDir; // Optional, can be null
    private final Path logFile;
    private final String ckanUrl;
    private final String ckanApiKey;
    private final boolean apiKeyFromEnv; // Track source of API Key
    private final boolean moveProcessed;
    private final List<String> relevantExtensions;
    private final boolean extractNestedZips;
    private final boolean createOrganizations;


    public ConfigLoader(Path configPath) throws IOException, IllegalArgumentException {
        this.configPath = Objects.requireNonNull(configPath, "configPath cannot be null").toAbsolutePath();
        this.properties = new Properties();
        this.executionDir = determineExecutionDirectory();
        logger.info("Resolved execution directory: {}", executionDir);

        if (!Files.isReadable(this.configPath)) {
            String errorMsg = String.format("Configuration file '%s' not found or not readable!", this.configPath);
            System.err.println("ERROR: " + errorMsg); // Fallback stderr logging
            logger.error(errorMsg);
            throw new IOException(errorMsg);
        }

        try (InputStream input = new FileInputStream(this.configPath.toFile())) {
            properties.load(input);
            logger.info("Configuration properties loaded from: {}", this.configPath);
            // Call the central loading and validation method
            // Note: Fields assigned here are marked final, fulfilling initialization requirement.
            this.sourceDir = getRequiredPathProperty(KEY_SOURCE_DIR);
            this.stagingDir = getRequiredPathProperty(KEY_STAGING_DIR);
            this.logFile = getRequiredPathProperty(KEY_LOG_FILE);
            this.ckanUrl = getRequiredProperty(KEY_CKAN_URL);
            this.ckanApiKey = loadCkanApiKey(); // Extracted logic for API key loading
            this.apiKeyFromEnv = determineApiKeySource(); // Extracted logic for tracking source
            this.processedDir = getOptionalPathProperty(KEY_PROCESSED_DIR);
            this.moveProcessed = loadAndValidateMoveProcessed(); // Extracted logic
            this.relevantExtensions = loadRelevantExtensions(); // Extracted logic
            this.extractNestedZips = getBooleanProperty(KEY_EXTRACT_NESTED_ZIPS, false);
            this.createOrganizations = getBooleanProperty(KEY_CREATE_ORGS, false);

            // Post-load validations
            validateUrl(this.ckanUrl, KEY_CKAN_URL);
            validateDirectories(); // Validates sourceDir, stagingDir, processedDir (if needed), log dir
            showSecurityWarning();
            logLoadedConfiguration();

        } catch (IOException e) {
            String errorMsg = String.format("Error reading configuration file '%s': %s", this.configPath, e.getMessage());
            logger.error(errorMsg, e);
            throw new IOException(errorMsg, e);
        } catch (IllegalArgumentException | IllegalStateException e) { // Catch validation/state errors
            String errorMsg = String.format("Invalid configuration in '%s': %s", this.configPath, e.getMessage());
            logger.error(errorMsg, e);
            throw e; // Re-throw validation/state exceptions directly
        }
    }

    // --- Extracted Loading Logic for Final Fields ---

    private String loadCkanApiKey() {
        String apiKeyFromEnvVar = System.getenv(ENV_CKAN_API_KEY);
        if (apiKeyFromEnvVar != null && !apiKeyFromEnvVar.trim().isEmpty()) {
            logger.info("Loaded CKAN API Key from environment variable '{}'", ENV_CKAN_API_KEY);
            return apiKeyFromEnvVar.trim();
        } else {
            logger.info("Loaded CKAN API Key from configuration file key '{}'", KEY_CKAN_API_KEY);
            return getRequiredProperty(KEY_CKAN_API_KEY); // Fallback to properties file
        }
    }

    private boolean determineApiKeySource() {
        // Based on whether the environment variable was present and non-empty
        String apiKeyFromEnvVar = System.getenv(ENV_CKAN_API_KEY);
        return (apiKeyFromEnvVar != null && !apiKeyFromEnvVar.trim().isEmpty());
    }

    private boolean loadAndValidateMoveProcessed() {
        boolean shouldMove = getBooleanProperty(KEY_MOVE_PROCESSED, false);
        // Use the already loaded 'processedDir' field for validation
        if (shouldMove && this.processedDir == null) {
            logger.warn("'{}' is true, but '{}' is not set or invalid in config file '{}'. Processed files will NOT be moved.",
                    KEY_MOVE_PROCESSED, KEY_PROCESSED_DIR, configPath);
            return false; // Disable if dir is invalid/missing
        }
        return shouldMove;
    }

    private List<String> loadRelevantExtensions() {
        String extensionsStr = properties.getProperty(KEY_RELEVANT_EXTENSIONS, "").trim();
        if (!extensionsStr.isEmpty()) {
            return Arrays.stream(extensionsStr.split(","))
                    .map(String::trim)
                    .filter(ext -> !ext.isEmpty() && ext.startsWith("."))
                    .map(String::toLowerCase)
                    .distinct()
                    .collect(Collectors.collectingAndThen(Collectors.toList(), Collections::unmodifiableList)); // Ensure unmodifiable
        } else {
            return Collections.emptyList();
        }
    }

    // --- Helper Methods (Largely unchanged, minor tweaks possible) ---

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
                    return path.getParent(); // Return parent directory of the JAR file
                } else {
                    logger.warn("CodeSource location is neither a directory nor a JAR file: {}. Falling back to CWD.", path);
                }
            } else {
                logger.warn("Could not get CodeSource location. Falling back to CWD.");
            }
        } catch (URISyntaxException | SecurityException | NullPointerException | InvalidPathException e) {
            logger.warn("Could not reliably determine execution directory (Error: {}). Falling back to CWD.", e.getMessage());
        }
        Path cwd = Paths.get(".").toAbsolutePath().normalize(); // Normalize CWD
        logger.info("Execution directory determined via fallback (CWD): {}", cwd);
        return cwd;
    }

    private String getRequiredProperty(String key) throws IllegalArgumentException {
        String value = properties.getProperty(key);
        if (value == null) return null;

        int commentIndex = value.indexOf('#');
        if (commentIndex != -1) {
            value = value.substring(0, commentIndex);
        }
        return value.trim();
    }

    private Path resolvePathProperty(String value) throws InvalidPathException {
        Path path = Paths.get(value);
        if (path.isAbsolute()) {
            logger.debug("Path '{}' is absolute.", path);
            return path.normalize();
        } else {
            Path resolvedPath = executionDir.resolve(value).normalize();
            logger.debug("Resolved relative path '{}' to '{}' based on execution directory '{}'.", value, resolvedPath, executionDir);
            return resolvedPath;
        }
    }

    private Path getRequiredPathProperty(String key) throws IllegalArgumentException {
        String pathStr = getRequiredProperty(key);
        try {
            return resolvePathProperty(pathStr);
        } catch (InvalidPathException e) {
            throw new IllegalArgumentException(String.format("Invalid path format for key '%s': '%s'", key, pathStr), e);
        }
    }

    private Path getOptionalPathProperty(String key) throws IllegalArgumentException {
        String pathStr = properties.getProperty(key, "").trim();
        if (pathStr.isEmpty()) {
            return null;
        }
        try {
            return resolvePathProperty(pathStr);
        } catch (InvalidPathException e) {
            logger.warn("Invalid path format for optional key '{}': '{}'. Ignoring setting.", key, pathStr, e);
            return null;
        }
    }

    private boolean getBooleanProperty(String key, boolean defaultValue) {
        String value = properties.getProperty(key);
        if (value == null) {
            return defaultValue;
        }
        String trimmedValue = value.trim().toLowerCase();
        // Using a switch expression (Java 14+) for clarity, fallback to if/else if needed
        return switch (trimmedValue) {
            case "true", "yes", "1" -> true;
            case "false", "no", "0" -> false;
            default -> {
                logger.warn("Unrecognized boolean value for key '{}': '{}'. Using default: {}", key, value, defaultValue);
                yield defaultValue;
            }
        };
        /* // Pre-Java 14 equivalent:
        if ("true".equals(trimmedValue) || "yes".equals(trimmedValue) || "1".equals(trimmedValue)) {
            return true;
        } else if ("false".equals(trimmedValue) || "no".equals(trimmedValue) || "0".equals(trimmedValue)) {
            return false;
        } else {
            logger.warn("Unrecognized boolean value for key '{}': '{}'. Using default: {}", key, value, defaultValue);
            return defaultValue;
        }
        */
    }

    // Helper for stricter URL validation using URI
    private void validateUrl(String urlString, String key) throws IllegalArgumentException {
        try {
            URI uri = new URI(urlString);
            // Basic check: scheme must be http or https
            String scheme = uri.getScheme();
            if (!"http".equalsIgnoreCase(scheme) && !"https".equalsIgnoreCase(scheme)) {
                throw new IllegalArgumentException(
                        String.format("Invalid URL scheme for key '%s': '%s'. Must be http or https.", key, urlString)
                );
            }
            // Could add further checks e.g., uri.getHost() != null if required
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(
                    String.format("Invalid URL syntax for key '%s': '%s'", key, urlString), e
            );
        }
    }

    // Helper to validate all required directories
    private void validateDirectories() throws IOException {
        validateDirectory("Source", sourceDir, true, false, true); // Check exists, readable, creatable
        validateDirectory("Staging", stagingDir, true, true, true); // Check exists, readable, writable, creatable

        if (moveProcessed && processedDir != null) { // Check processedDir only if move is true AND path is set
            validateDirectory("Processed", processedDir, true, true, true); // Check exists, readable, writable, creatable
        }

        Path logParentDir = (logFile != null) ? logFile.getParent() : null;
        if (logParentDir != null) {
            validateDirectory("Log Directory", logParentDir, true, true, true); // Ensure log dir exists and is writable
        } else if (logFile != null) {
            validateDirectory("Execution (for Logging)", executionDir, false, true, false); // Check execution dir writable if log file is in root
        } else {
            // This state should be impossible due to logFile being required, but handle defensively.
            throw new IllegalStateException("Log file path is null after loading configuration.");
        }
    }


    // Helper for basic directory validation and creation attempt (Unchanged)
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
                    } catch (IOException | SecurityException e) {
                        throw new IOException("Failed to create " + dirDesc + ": " + e.getMessage(), e);
                    }
                } else {
                    throw new IOException(dirDesc + " does not exist and creation was not attempted.");
                }
            }

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
                throw new IllegalStateException(dirDesc + " does not exist, validation logic error.");
            }
        } catch (SecurityException e) {
            throw new IOException("Permission error accessing " + dirDesc + ": " + e.getMessage(), e);
        }
    }

    // Logs security warnings related to API key storage (Unchanged)
    private void showSecurityWarning() {
        boolean keySeemsInsecure = false;
        if (!apiKeyFromEnv) {
            keySeemsInsecure = true;
            logger.warn("CKAN API Key was loaded directly from the configuration file: {}", configPath);
            // Use Objects.equals for safe null comparison, although ckanApiKey should be non-null if !apiKeyFromEnv
            if (Objects.equals("YOUR_CKAN_API_KEY_HERE", ckanApiKey) || ckanApiKey == null || ckanApiKey.isEmpty()) {
                logger.error("CKAN API Key is NOT set correctly in {}. Pipeline WILL fail authorization.", configPath);
            } else if (ckanApiKey.length() < 20) {
                logger.warn("The CKAN API key configured in {} seems very short. Please verify it.", configPath);
            }
        } else {
            if (Objects.equals("YOUR_CKAN_API_KEY_HERE", ckanApiKey) || ckanApiKey == null || ckanApiKey.isEmpty()) {
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

    // Logs a summary of the loaded (non-sensitive) configuration (Unchanged)
    private void logLoadedConfiguration() {
        logger.info("--- Loaded Configuration Summary ---");
        logger.info("{}: {}", KEY_SOURCE_DIR, sourceDir);
        logger.info("{}: {}", KEY_STAGING_DIR, stagingDir);
        logger.info("{}: {}", KEY_PROCESSED_DIR, processedDir != null ? processedDir : "N/A");
        logger.info("{}: {}", KEY_LOG_FILE, logFile);
        logger.info("{}: {}", KEY_CKAN_URL, ckanUrl);
        logger.info("CKAN API Key Source: {}", apiKeyFromEnv ? "Environment Variable ("+ENV_CKAN_API_KEY+")" : "Config File ("+KEY_CKAN_API_KEY+")");
        logger.info("{}: {}", KEY_MOVE_PROCESSED, moveProcessed);
        logger.info("{}: {}", KEY_RELEVANT_EXTENSIONS, relevantExtensions); // Already unmodifiable list
        logger.info("{}: {}", KEY_EXTRACT_NESTED_ZIPS, extractNestedZips);
        logger.info("{}: {}", KEY_CREATE_ORGS, createOrganizations);
        logger.info("--- End Configuration Summary ---");
    }


    // --- Public Getters (Now return final fields) ---
    public Path getConfigPath() { return configPath; }
    public Path getSourceDir() { return sourceDir; }
    public Path getStagingDir() { return stagingDir; }

    public Path getProcessedDir() { return processedDir; }

    public Path getLogFile() { return logFile; }
    public String getCkanUrl() { return ckanUrl; }

    public String getCkanApiKey() { return ckanApiKey; }

    public boolean isMoveProcessed() { return moveProcessed; }

    public List<String> getRelevantExtensions() { return relevantExtensions; } // Already returning unmodifiable list

    public boolean isExtractNestedZips() { return extractNestedZips; }
    public boolean isCreateOrganizations() { return createOrganizations; }
    public Path getExecutionDir() { return executionDir; }
}
// Save as: src/Main.java

import config.ConfigLoader;
import pipeline.Pipeline;
import util.LoggingUtil;

import org.slf4j.Logger;

import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects; // Make sure Objects is imported if needed elsewhere

/**
 * Main entry point for the CKAN Data Pipeline application.
 * Handles command-line arguments, configuration loading, pipeline initialization,
 * execution, and top-level error handling.
 */
public class Main {

    // Standard class-based logger initialization
    private static final Logger logger = LoggingUtil.getLogger(Main.class);

    public static void main(String[] args) {

        // FIX: Removed call to LoggingUtil.setupBasicLogging() as the method was removed.
        // Logging initialization now relies entirely on the SLF4J binding finding
        // its configuration file (e.g., logback.xml, logging.properties) on the classpath.
        // Ensure such a configuration file exists in src/main/resources.

        logger.info("==================================================");
        logger.info("Starting CKAN Data Pipeline application...");
        logger.info("Java Version: {}", System.getProperty("java.version"));
        logger.info("Working Directory: {}", Paths.get(".").toAbsolutePath().normalize());
        logger.info("==================================================");


        Path configFilePath = null;
        ConfigLoader config = null;

        try {
            // Determine config file path (default or from args)
            String configArg = (args.length > 0) ? args[0] : "config.properties";
            configFilePath = Paths.get(configArg).toAbsolutePath().normalize();
            logger.info("Using configuration file path: {}", configFilePath);

            // 1. Load Configuration
            logger.info("Loading configuration...");
            config = new ConfigLoader(configFilePath);

            logger.info("Configuration loaded successfully.");
            // Assumes getExecutionDir() getter exists in ConfigLoader
            logger.info("Effective execution directory: {}", config.getExecutionDir());

            // 2. Initialize Pipeline
            logger.info("Initializing pipeline...");
            Pipeline pipeline = new Pipeline(config);

            // 3. Run Pipeline
            logger.info("Starting pipeline run...");
            pipeline.run();

            logger.info("Pipeline execution finished normally.");
            logger.info("==================================================");
            System.exit(0); // Success

        } catch (InvalidPathException e) {
            String configInput = (args.length > 0 ? args[0] : "config.properties");
            logger.error("FATAL: Invalid configuration file path provided: '{}'. Error: {}", configInput, e.getMessage(), e);
            System.err.println("\nFATAL: Invalid configuration file path specified: " + configInput);
            System.err.println("Error: " + e.getMessage());
            System.exit(2); // Config path error exit code
        } catch (Exception e) {
            // Catch all other exceptions during setup or run
            logger.error("FATAL: Pipeline execution failed due to an unhandled exception: {}", e.getMessage(), e);
            System.err.println("\nFATAL: An unexpected error occurred. Check logs for details.");
            System.err.println("Error Type: " + e.getClass().getName());
            System.err.println("Error Message: " + e.getMessage());
            if (configFilePath != null) { System.err.println("Config file used: " + configFilePath); }
            else if (args.length > 0) { System.err.println("Config file argument: " + args[0]); }
            else { System.err.println("Config file: config.properties (default)"); }
            logger.info("==================================================");
            System.exit(1); // General error exit code
        }
    }
    // --- TODO: Ensure ConfigLoader.java has ---
    /*
    public Path getExecutionDir() {
        return executionDir;
    }
    */
}
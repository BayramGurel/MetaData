import config.ConfigLoader;
import pipeline.Pipeline;
import util.LoggingUtil;

import org.slf4j.Logger;

import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Main entry point for the CKAN Data Pipeline application.
 * Handles command-line arguments, configuration loading, pipeline initialization,
 * execution, and top-level error handling.
 */
public class Main {

    // Standard class-based logger initialization
    private static final Logger logger = LoggingUtil.getLogger(Main.class);

    public static void main(String[] args) {

        // Logging initialization relies entirely on the SLF4J binding finding
        // its configuration file (e.g., logback.xml, logging.properties) on the classpath.
        // Ensure such a configuration file exists (e.g., in src/main/resources).

        logger.info("==================================================");
        logger.info("Starting CKAN Data Pipeline application...");
        logger.info("Java Version: {}", System.getProperty("java.version"));
        logger.info("Working Directory: {}", Paths.get(".").toAbsolutePath().normalize());
        logger.info("==================================================");


        Path configFilePath = null;
        ConfigLoader config = null;

        try {
            // Determine config file path (default or from args)
            String configArg = (args.length > 0) ? args[0] : "src/config.properties";
            configFilePath = Paths.get(configArg).toAbsolutePath().normalize();
            logger.info("Using configuration file path: {}", configFilePath);

            // 1. Load Configuration
            logger.info("Loading configuration...");
            config = new ConfigLoader(configFilePath);

            logger.info("Configuration loaded successfully.");
            // NOTE: This relies on getExecutionDir() being added to ConfigLoader as per TODO
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
            // Provide context about the config file used
            if (configFilePath != null) {
                System.err.println("Config file used: " + configFilePath);
            } else if (args.length > 0) {
                System.err.println("Config file argument: " + args[0]);
            } else {
                System.err.println("Config file: config.properties (default)");
            }
            logger.info("==================================================");
            System.exit(1); // General error exit code
        }
    }
    // --- TODO: Ensure ConfigLoader.java has the getExecutionDir() method ---
    /*
    // Add this public getter method to your ConfigLoader.java class:
    public Path getExecutionDir() {
        return executionDir; // Assuming 'executionDir' is the instance field holding the path
    }
    */
} // End of Main class
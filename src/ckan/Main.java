// src/ckan/Main.java
package ckan;

import ckan.util.LoggingUtil;
import org.slf4j.Logger; // Use SLF4J logger

import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Main {

    // Early logger for startup issues before proper config/file logging might be set up.
    // Using the class name directly for the early logger.
    private static final Logger earlyLogger = LoggingUtil.getLogger(Main.class.getName());

    public static void main(String[] args) {
        // Basic SLF4J setup (using the binding, e.g., simple console via slf4j-simple or JUL via slf4j-jdk14)
        // More complex config relies on binding-specific files (e.g., logback.xml) placed in src/main/resources
        LoggingUtil.setupBasicLogging(); // Call basic setup if needed by your logging util/framework
        earlyLogger.info("Starting CKAN Data Pipeline application...");
        earlyLogger.info("Java Version: {}", System.getProperty("java.version"));
        earlyLogger.info("Working Directory: {}", Paths.get(".").toAbsolutePath().normalize());


        Path configFilePath = null;
        ConfigLoader config = null;

        try {
            // Determine config file path (default or from args)
            String configArg = (args.length > 0) ? args[0] : "config.properties"; // Default name if no arg provided
            configFilePath = Paths.get(configArg).toAbsolutePath(); // Make absolute relative to CWD if not absolute already
            earlyLogger.info("Attempting to use configuration file path: {}", configFilePath);

            // 1. Load Configuration (throws exceptions on failure)
            config = new ConfigLoader(configFilePath); // This now also validates/creates directories

            // Logging SHOULD now be configured based on ConfigLoader potentially resolving logFile path
            // and the SLF4J binding finding its configuration (e.g., logback.xml).
            // Get logger instance again using the proper class - it might now use file handlers etc.
            Logger logger = LoggingUtil.getLogger(Main.class);
            logger.info("Configuration loaded successfully. Logging should be initialized based on binding configuration.");
            // Log the effective execution directory determined by ConfigLoader
            logger.info("Effective execution directory: {}", config.determineExecutionDirectory());

            // 2. Initialize Pipeline (throws exceptions on failure, e.g., invalid CKAN URL)
            logger.info("Initializing pipeline...");
            Pipeline pipeline = new Pipeline(config);

            // 3. Run Pipeline
            logger.info("Starting pipeline run...");
            pipeline.run(); // run() method handles its own summary logging

            logger.info("Pipeline execution finished normally.");
            System.exit(0); // Explicitly exit with success code

        } catch (InvalidPathException e) {
            earlyLogger.error("FATAL: Invalid configuration file path provided: '{}'. Error: {}", (args.length > 0 ? args[0] : "config.properties"), e.getMessage());
            System.err.println("FATAL: Invalid configuration file path: " + e.getMessage());
            System.exit(2); // Specific exit code for config path error
        } catch (Exception e) {
            // Catch errors during config loading, pipeline init, or the run itself
            // Use earlyLogger as the configured one might have failed if the error was early (e.g., config loading)
            earlyLogger.error("FATAL: Pipeline execution failed due to an unhandled exception: {}", e.getMessage(), e);
            System.err.println("\nFATAL: An unexpected error occurred. Check logs for details.");
            System.err.println("Error Type: " + e.getClass().getName());
            System.err.println("Error Message: " + e.getMessage());
            if (configFilePath != null) {
                System.err.println("Config file used: " + configFilePath);
            } else if (args.length > 0) {
                System.err.println("Config file argument: " + args[0]);
            } else {
                System.err.println("Config file: config.properties (default)");
            }
            System.exit(1); // Exit with general error code
        }
    }
}
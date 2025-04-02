// Save as: src/com/yourcompany/util/LoggingUtil.java
package util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class providing convenient access to SLF4J {@link Logger} instances.
 *
 * <p>This class simplifies obtaining loggers via SLF4J. The actual logging configuration
 * (e.g., output format, level, file rotation) is managed externally through the
 * configuration file of the chosen SLF4J binding found on the classpath
 * (e.g., {@code logback.xml}, {@code log4j2.xml}, or {@code logging.properties} for slf4j-jdk14).</p>
 *
 * <p><b>Note:</b> While this utility can provide consistency, directly using
 * {@code LoggerFactory.getLogger(...)} within classes is also a common and perfectly
 * valid practice.</p>
 */
public final class LoggingUtil {

    // Private constructor to prevent instantiation of this utility class.
    private LoggingUtil() {
        throw new UnsupportedOperationException("Utility class - cannot be instantiated");
    }

    /**
     * Gets an SLF4J Logger named after the specified class.
     * <p>
     * This is the standard approach for associating log messages with the class
     * where they originate.
     * </p>
     *
     * @param clazz The class for which the logger is needed. Must not be null.
     * @return The {@link Logger} instance associated with the given class.
     * @throws NullPointerException if clazz is null.
     */
    public static Logger getLogger(Class<?> clazz) {
        // LoggerFactory internally handles null check, but good practice to document expectation
        // if (clazz == null) throw new NullPointerException("Class cannot be null");
        return LoggerFactory.getLogger(clazz);
    }

    /**
     * Gets an SLF4J Logger associated with the specified name.
     * <p>
     * Useful for creating loggers for specific subsystems or functionalities
     * that may not directly correspond to a single class.
     * </p>
     *
     * @param name The explicit name for the logger. Should not be null or empty.
     * @return The {@link Logger} instance associated with the given name.
     * @throws NullPointerException if name is null.
     */
    public static Logger getLogger(String name) {
        // LoggerFactory internally handles null check
        // if (name == null || name.trim().isEmpty()) {
        //     throw new IllegalArgumentException("Logger name cannot be null or empty");
        // }
        return LoggerFactory.getLogger(name);
    }

    /*
     * Removed the setupBasicLogging() method.
     * Rationale:
     * 1. Interference: Programmatic configuration here can interfere with or be overridden
     * by standard external configuration files (logback.xml, log4j2.xml), leading to confusion.
     * 2. Limited Scope: It primarily targeted java.util.logging (via slf4j-jdk14), which is less
     * commonly configured programmatically compared to using a logging.properties file.
     * 3. Best Practice: Relying on the standard configuration mechanisms of the chosen SLF4J
     * binding (Logback, Log4j2, etc.) is more robust, flexible, and conventional. Add the
     * appropriate configuration file to your project's resources directory instead.
     * 4. Global State: Modifying system properties (as the example did) is a global change
     * that can have unintended side effects.
     */
}
// Save as: src/old.util/LoggingUtil.java
package old.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class LoggingUtil {

    // Private constructor to prevent instantiation of this utility class.
    private LoggingUtil() {
        throw new UnsupportedOperationException("Utility class - cannot be instantiated");
    }

    public static Logger getLogger(Class<?> clazz) {
        // LoggerFactory internally handles null check, documenting expectation.
        // If explicit check desired: if (clazz == null) throw new NullPointerException("Class cannot be null");
        return LoggerFactory.getLogger(clazz);
    }

    public static Logger getLogger(String name) {
        // LoggerFactory internally handles null/empty check, documenting expectation.
        // If explicit check desired: if (name == null || name.trim().isEmpty()) throw new IllegalArgumentException("Logger name cannot be null or empty");
        return LoggerFactory.getLogger(name);
    }
}
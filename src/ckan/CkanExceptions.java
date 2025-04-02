// Save as: src/ckan/CkanExceptions.java
package ckan; // Using package name from your snippet

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Container class for CKAN client-related custom exceptions.
 * (Javadoc remains the same...)
 */
public final class CkanExceptions {

    // Private constructor remains the same...
    private CkanExceptions() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    /**
     * Base class for specific CKAN client-related IOExceptions.
     * (Class remains the same...)
     */
    public static class CkanException extends IOException {
        public CkanException(String message) {
            super(message);
        }
        public CkanException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Represents a CKAN 'Not Found' error (HTTP 404).
     * (Class remains the same...)
     */
    public static final class CkanNotFoundException extends CkanException {
        public CkanNotFoundException(String message) {
            super(message);
        }
        public CkanNotFoundException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Represents a CKAN Authorization error (HTTP 401/403).
     * (Class remains the same...)
     */
    public static final class CkanAuthorizationException extends CkanException {
        public CkanAuthorizationException(String message) {
            super(message);
        }
        public CkanAuthorizationException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Represents a CKAN Validation error (HTTP 400/409).
     * (Class remains the same...)
     */
    public static final class CkanValidationException extends CkanException {
        private final transient Map<String, Object> errorDict;
        public CkanValidationException(String message, Map<String, Object> errorDict) {
            super(message);
            this.errorDict = (errorDict == null || errorDict.isEmpty()) ? null : Map.copyOf(errorDict);
        }
        public CkanValidationException(String message, Map<String, Object> errorDict, Throwable cause) {
            super(message, cause);
            this.errorDict = (errorDict == null || errorDict.isEmpty()) ? null : Map.copyOf(errorDict);
        }
        public Map<String, Object> getErrorDict() { return errorDict; }
        @Override public String getMessage() { String baseMessage = super.getMessage(); if (errorDict != null && !errorDict.isEmpty()) { return baseMessage + ": " + errorDict.toString(); } return baseMessage; }
    }

    /**
     * Represents an error connecting to or communicating with the CKAN instance (Network, HTTP 5xx).
     */
    public static final class CkanConnectionException extends CkanException {
        /**
         * Constructs a CkanConnectionException.
         * @param message A message describing the connection or communication failure.
         * @param cause The underlying network/IO exception, or null if derived from status code only.
         */
        public CkanConnectionException(String message, Throwable cause) {
            // FIX: Removed Objects.requireNonNull(cause) to allow null causes,
            // matching usage in CkanHandler for 5xx status code errors.
            super(message, cause);
        }
    }

    /**
     * Indicates that a CKAN API call reported success but unexpectedly returned
     * a null 'result' field when data was expected based on the TypeReference.
     * (Class remains the same...)
     */
    public static final class CkanUnexpectedNullResultException extends CkanException {
        public CkanUnexpectedNullResultException(String message) {
            super(message);
        }
    }
}
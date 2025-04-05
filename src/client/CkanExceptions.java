// Save as: src/ckan/CkanExceptions.java
package client; // Using package name from your snippet

import java.io.IOException;
import java.util.Map;

/**
 * Container class for CKAN client-related custom exceptions.
 */
public final class CkanExceptions {

    // Private constructor to prevent instantiation
    private CkanExceptions() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    /**
     * Base class for specific CKAN client-related IOExceptions.
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
     * Contains details about the validation failure.
     */
    public static final class CkanValidationException extends CkanException {
        // transient: Good practice if exception serialization isn't a primary goal
        // or if map contents aren't guaranteed serializable.
        private final transient Map<String, Object> errorDict;

        public CkanValidationException(String message, Map<String, Object> errorDict) {
            super(message);
            // Store an immutable copy to prevent external modification
            this.errorDict = (errorDict == null || errorDict.isEmpty()) ? null : Map.copyOf(errorDict);
        }

        public CkanValidationException(String message, Map<String, Object> errorDict, Throwable cause) {
            super(message, cause);
            // Store an immutable copy
            this.errorDict = (errorDict == null || errorDict.isEmpty()) ? null : Map.copyOf(errorDict);
        }

        /**
         * Gets the detailed error dictionary returned by CKAN, if available.
         * @return An immutable Map containing validation error details, or null if not provided/empty.
         */
        public Map<String, Object> getErrorDict() {
            return errorDict;
        }

        /**
         * Appends the error dictionary details to the exception message, if available.
         */
        @Override
        public String getMessage() {
            String baseMessage = super.getMessage();
            if (errorDict != null && !errorDict.isEmpty()) {
                // Append dict details for better logging/debugging
                return baseMessage + ": " + errorDict.toString();
            }
            return baseMessage;
        }
    }

    /**
     * Represents an error connecting to or communicating with the CKAN instance
     * (Network issues, DNS resolution failures, HTTP 5xx server errors).
     */
    public static final class CkanConnectionException extends CkanException {
        /**
         * Constructs a CkanConnectionException.
         * @param message A message describing the connection or communication failure.
         * @param cause The underlying network/IO exception, or null if derived from status code only (e.g., HTTP 5xx).
         */
        public CkanConnectionException(String message, Throwable cause) {
            // Allows null cause for errors derived from HTTP status codes directly
            super(message, cause);
        }
    }

    /**
     * Indicates that a CKAN API call reported success (e.g., HTTP 200 OK)
     * but unexpectedly returned a null 'result' field in the JSON response
     * when data was expected based on the TypeReference used for deserialization.
     */
    public static final class CkanUnexpectedNullResultException extends CkanException {
        public CkanUnexpectedNullResultException(String message) {
            super(message);
        }
    }
}
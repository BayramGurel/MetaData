// Save as: src/ckan/CkanHandler.java
package client; // Using package name from your snippet

import client.CkanExceptions.*;
import util.LoggingUtil; // Assuming LoggingUtil is in 'util' package

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature; // Import for configuration
import com.fasterxml.jackson.databind.ObjectMapper;
// import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule; // Example import if needed

import org.slf4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
// import java.io.UncheckedIOException; // Alternative for wrapping IOException
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Handles communication with a CKAN instance via its API (v3 Action API).
 * Provides methods for common actions like managing organizations, datasets, and resources.
 * Includes support for streaming large file uploads.
 * This class is final and designed to be thread-safe after construction.
 */
public final class CkanHandler { // Made class final

    private static final Logger logger = LoggingUtil.getLogger(CkanHandler.class);
    private static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofSeconds(90);
    private static final String DEFAULT_USER_AGENT = "JavaCkanPipelineClient/1.0";
    private static final String CRLF = "\r\n";

    // --- CKAN Action Name Constants ---
    private static final String ACTION_SITE_READ = "site_read";
    private static final String ACTION_ORG_SHOW = "organization_show";
    private static final String ACTION_ORG_CREATE = "organization_create";
    private static final String ACTION_PACKAGE_SHOW = "package_show";
    private static final String ACTION_PACKAGE_CREATE = "package_create";
    private static final String ACTION_PACKAGE_PATCH = "package_patch";
    private static final String ACTION_RESOURCE_CREATE = "resource_create";
    private static final String ACTION_RESOURCE_UPDATE = "resource_update";
    // Add more action names as needed...

    private final String ckanApiUrlBase;
    private final String apiKey;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    public CkanHandler(String ckanUrl, String apiKey) {
        Objects.requireNonNull(ckanUrl, "CKAN URL cannot be null");
        if (ckanUrl.trim().isEmpty()) { throw new IllegalArgumentException("CKAN URL cannot be empty."); }
        if (!ckanUrl.trim().toLowerCase().startsWith("http")) { throw new IllegalArgumentException("CKAN URL must start with http:// or https://"); }
        String baseUrl = ckanUrl.trim();
        // Ensure base URL ends with /api/3/action/
        this.ckanApiUrlBase = (baseUrl.endsWith("/api/3/action/") ? baseUrl :
                (baseUrl.endsWith("/") ? baseUrl : baseUrl + "/") + "api/3/action/");

        this.apiKey = (apiKey != null) ? apiKey.trim() : "";
        if (this.apiKey.isEmpty() || "YOUR_CKAN_API_KEY_HERE".equalsIgnoreCase(this.apiKey)) {
            logger.warn("CKAN API Key is missing, empty, or using a placeholder value. API calls requiring authentication WILL fail.");
        }

        this.httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(DEFAULT_REQUEST_TIMEOUT)
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();

        this.objectMapper = new ObjectMapper();
        // Configure ObjectMapper for robustness against API changes
        this.objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        // Optionally register JavaTimeModule if needed:
        // this.objectMapper.registerModule(new JavaTimeModule());

        logger.info("CKAN Handler initialized. API Base: {}", this.ckanApiUrlBase);
    }

    public void testConnection() throws CkanException, IOException {
        logger.info("Attempting test connection to CKAN (site_read)...");
        try {
            // Expecting Map<String, Object> which includes CKAN version info etc.
            sendRequest(ACTION_SITE_READ, null, "GET", new TypeReference<Map<String, Object>>() {});
            logger.info("Test connection successful.");
        } catch (CkanException e) {
            logger.error("Test connection failed: {}", e.getMessage());
            throw e; // Re-throw specific CKAN exception
        } catch (IOException e) {
            logger.error("Test connection failed due to IO error: {}", e.getMessage(), e);
            throw e; // Re-throw IO exception
        }
    }

    // --- Internal Request Sending Logic ---

    // Convenience overloads
    private <T> T sendRequest(String action, String method, TypeReference<T> responseTypeRef) throws CkanException, IOException {
        return sendRequestInternal(action, null, method, responseTypeRef, null, null);
    }
    private <T> T sendRequest(String action, Map<String, Object> data, String method, TypeReference<T> responseTypeRef) throws CkanException, IOException {
        return sendRequestInternal(action, data, method, responseTypeRef, null, null);
    }

    // Overload specifically for file uploads (now uses streaming)
    private <T> T sendUploadRequest(String action, Map<String, Object> data, Path fileToUpload, String uploadFieldName, TypeReference<T> responseTypeRef) throws CkanException, IOException {
        Objects.requireNonNull(fileToUpload, "File to upload cannot be null.");
        Objects.requireNonNull(uploadFieldName, "Upload field name cannot be null.");
        if (!Files.isReadable(fileToUpload)) { // Check readability, isRegularFile implicitly checked by newInputStream
            throw new IOException("File to upload must be readable: " + fileToUpload);
        }
        if (uploadFieldName.isBlank()) {
            throw new IllegalArgumentException("Upload field name cannot be blank.");
        }
        return sendRequestInternal(action, data, "POST", responseTypeRef, fileToUpload, uploadFieldName);
    }


    // Core request method - Handles GET, POST (JSON), and POST (Multipart Streaming)
    private <T> T sendRequestInternal(String action, Map<String, Object> data, String method, TypeReference<T> responseTypeRef, Path fileToUpload, String uploadFieldName) throws CkanException, IOException {
        URI uri;
        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                .timeout(DEFAULT_REQUEST_TIMEOUT)
                .header("User-Agent", DEFAULT_USER_AGENT);

        if (!this.apiKey.isEmpty()) { // Check if API key exists before adding header
            requestBuilder.header("Authorization", this.apiKey);
        } else {
            logger.debug("Sending request to {} without Authorization header (API key not provided).", action);
        }

        HttpRequest request;
        long requestStartTime = System.nanoTime();

        try {
            if ("POST".equalsIgnoreCase(method)) {
                uri = URI.create(ckanApiUrlBase + action); // URI for POST actions
                if (fileToUpload != null) {
                    // --- Streaming Multipart Upload ---
                    String boundary = "Boundary-" + UUID.randomUUID().toString();
                    requestBuilder.header("Content-Type", "multipart/form-data; boundary=" + boundary);

                    // FIX 1: Handle potential IOException from buildMultipartStream inside the lambda
                    HttpRequest.BodyPublisher multipartBodyPublisher = HttpRequest.BodyPublishers.ofInputStream(
                            () -> {
                                try {
                                    // buildMultipartStream is declared with 'throws IOException'
                                    return buildMultipartStream(boundary, data, uploadFieldName, fileToUpload);
                                } catch (IOException e) {
                                    // Wrap the checked IOException in an unchecked exception
                                    logger.error("Failed to prepare multipart input stream for action {}: {}", action, e.getMessage(), e);
                                    throw new RuntimeException("Failed to build multipart input stream: " + e.getMessage(), e);
                                    // Alternatively use: throw new java.io.UncheckedIOException(e);
                                }
                            }
                    );

                    request = requestBuilder.uri(uri).POST(multipartBodyPublisher).build();
                    logger.debug("POST (Streaming Multipart) Request to: {} with fields: {} and file: {}", uri, data != null ? data.keySet() : "none", fileToUpload.getFileName());

                } else {
                    // --- Standard JSON POST ---
                    requestBuilder.header("Content-Type", "application/json; charset=utf-8");
                    String requestBody = (data != null && !data.isEmpty()) ? objectMapper.writeValueAsString(data) : "{}";
                    logger.trace("POST Request to: {} with body: {}", uri, requestBody);
                    request = requestBuilder.uri(uri).POST(HttpRequest.BodyPublishers.ofString(requestBody, StandardCharsets.UTF_8)).build();
                }
            } else { // Assuming GET for other methods
                // --- GET Request ---
                URI baseUri = URI.create(ckanApiUrlBase + action);
                URIBuilder uriBuilder = new URIBuilder(baseUri); // Use helper to build URI with query params
                if (data != null && !data.isEmpty()) {
                    data.forEach((key, value) -> {
                        if (value != null) { // Handle null values gracefully
                            uriBuilder.addParameter(key, String.valueOf(value));
                        }
                    });
                }
                try {
                    uri = uriBuilder.build(); // Build final URI with encoded params
                } catch (URISyntaxException e) {
                    // This should be rare with controlled inputs but handle defensively
                    throw new CkanException("Internal error: Failed to build GET URI for " + action, e);
                }
                requestBuilder.uri(uri);
                logger.debug("GET Request to: {}", uri);
                request = requestBuilder.GET().build();
            }

            // Send the request and handle response
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            long durationMillis = Duration.ofNanos(System.nanoTime() - requestStartTime).toMillis();
            logger.info("CKAN call '{}' completed with status {} in {} ms", action, response.statusCode(), durationMillis);

            return handleResponse(response, action, responseTypeRef);

        } catch (JsonProcessingException e) {
            // Error serializing JSON request body
            throw new CkanException("Internal error: Failed to create JSON request for " + action, e);
        } catch (IOException e) {
            // Network errors, DNS errors, connection refused, etc.
            logger.error("Connection or I/O error during CKAN request for action {}: {}", action, e.getMessage(), e);
            if (e.getCause() instanceof InterruptedException) {
                Thread.currentThread().interrupt();
                logger.warn("CKAN request thread interrupted for action: {}", action);
                // Fall through to throw CkanConnectionException below
            }
            throw new CkanConnectionException("Communication error with CKAN for action " + action + ": " + e.getMessage(), e);
        } catch (InterruptedException e) {
            // Thread interrupted while waiting for response
            Thread.currentThread().interrupt();
            logger.warn("CKAN request thread interrupted for action: {}", action);
            throw new CkanConnectionException("CKAN request interrupted for action " + action, e);
        }
        // Catch RuntimeException from the lambda, if needed, although HttpClient might wrap it
        catch (RuntimeException e) {
            logger.error("Runtime error during CKAN request setup (potentially from stream supplier) for action {}: {}", action, e.getMessage(), e);
            // Decide how to handle - maybe wrap in CkanException or let it propagate
            // Wrapping might hide the original cause slightly but fits the exception hierarchy
            throw new CkanException("Internal setup error during request for action " + action + ": " + e.getMessage(), e);
        }
    }


    // Helper method to build the multipart InputStream for streaming uploads
    // Declares throws IOException as it uses file operations
    private InputStream buildMultipartStream(String boundary, Map<String, Object> textParts, String fileFieldName, Path filePath) throws IOException {
        List<InputStream> streams = new ArrayList<>();
        byte[] boundaryBytes = ("--" + boundary + CRLF).getBytes(StandardCharsets.UTF_8);
        byte[] finalBoundaryBytes = ("--" + boundary + "--" + CRLF).getBytes(StandardCharsets.UTF_8);
        byte[] crlfBytes = CRLF.getBytes(StandardCharsets.UTF_8);

        // Add text parts
        if (textParts != null) {
            for (Map.Entry<String, Object> entry : textParts.entrySet()) {
                if (entry.getValue() != null) {
                    streams.add(new ByteArrayInputStream(boundaryBytes)); // Part boundary

                    String header = String.format("Content-Disposition: form-data; name=\"%s\"%s", entry.getKey(), CRLF);
                    header += String.format("Content-Type: text/plain; charset=UTF-8%s%s", CRLF, CRLF);
                    streams.add(new ByteArrayInputStream(header.getBytes(StandardCharsets.UTF_8))); // Part headers

                    streams.add(new ByteArrayInputStream(String.valueOf(entry.getValue()).getBytes(StandardCharsets.UTF_8))); // Part value
                    streams.add(new ByteArrayInputStream(crlfBytes)); // CRLF after part value
                }
            }
        }

        // Add file part
        streams.add(new ByteArrayInputStream(boundaryBytes)); // File part boundary
        String fileName = filePath.getFileName().toString();
        // probeContentType can throw IOException
        String mimeType = Files.probeContentType(filePath);
        mimeType = (mimeType == null) ? "application/octet-stream" : mimeType;
        String fileHeader = String.format("Content-Disposition: form-data; name=\"%s\"; filename=\"%s\"%s", fileFieldName, fileName, CRLF);
        fileHeader += String.format("Content-Type: %s%s%s", mimeType, CRLF, CRLF);
        streams.add(new ByteArrayInputStream(fileHeader.getBytes(StandardCharsets.UTF_8))); // File part headers

        // Add the actual file stream - Files.newInputStream can throw IOException
        streams.add(Files.newInputStream(filePath)); // The file content stream itself
        streams.add(new ByteArrayInputStream(crlfBytes)); // CRLF after file content

        // Add final boundary
        streams.add(new ByteArrayInputStream(finalBoundaryBytes));

        // Chain all the streams together using SequenceInputStream
        InputStream resultStream = new ByteArrayInputStream(new byte[0]);
        for (InputStream stream : streams) {
            resultStream = new SequenceInputStream(resultStream, stream);
        }
        return resultStream;
    }


    // URIBuilder helper class (remains unchanged)
    private static class URIBuilder {
        private final URI baseUri;
        private final List<Map.Entry<String, String>> params = new ArrayList<>();

        URIBuilder(URI baseUri) { this.baseUri = baseUri; }

        URIBuilder addParameter(String key, String value) {
            params.add(Map.entry(key, value));
            return this;
        }

        URI build() throws URISyntaxException {
            if (params.isEmpty()) return baseUri;
            String q = baseUri.getRawQuery();
            StringBuilder s = new StringBuilder(q == null ? "" : q);
            if (s.length() > 0 && !params.isEmpty()) s.append('&');
            s.append(params.stream()
                    .map(e -> URLEncoder.encode(e.getKey(), StandardCharsets.UTF_8) + "=" + URLEncoder.encode(e.getValue(), StandardCharsets.UTF_8))
                    .collect(Collectors.joining("&")));
            return new URI(baseUri.getScheme(), baseUri.getAuthority(), baseUri.getPath(), s.toString(), baseUri.getFragment());
        }
    }


    // handleResponse method (remains largely unchanged, includes previous fixes)
    @SuppressWarnings("unchecked") // For casting Object to Map and List
    private <T> T handleResponse(HttpResponse<String> response, String action, TypeReference<T> responseTypeRef) throws CkanException {
        int statusCode = response.statusCode();
        String responseBody = response.body();

        logger.debug("Response from action '{}' ({}) Headers: {}", action, statusCode, response.headers().map());
        if (logger.isTraceEnabled()) {
            logger.trace("Response from action '{}' ({}) Body: {}", action, statusCode, responseBody);
        } else if (responseBody != null && !responseBody.isEmpty()) {
            // Log truncated body for DEBUG level to avoid flooding logs
            logger.debug("Response from action '{}' ({}) Body: {}", action, statusCode,
                    responseBody.length() > 1024 ? responseBody.substring(0, 1024) + "... (truncated)" : responseBody);
        }

        Map<String, Object> ckanResponse;
        try {
            // Handle empty body - ok if expecting Void, error otherwise
            if (responseBody == null || responseBody.isBlank()) {
                if (statusCode >= 200 && statusCode < 300) { // Success range
                    if (Void.class.equals(responseTypeRef.getType())) {
                        return null; // Expected empty body for Void response
                    } else {
                        throw new CkanException(String.format(
                                "CKAN action '%s' succeeded (%d) but returned an empty response body when expecting type %s.",
                                action, statusCode, responseTypeRef.getType()));
                    }
                } else { // Failure range
                    throw new CkanException(String.format(
                            "CKAN action '%s' failed (%d) and returned an empty response body.",
                            action, statusCode));
                }
            }
            // Parse non-empty body as a generic map first
            ckanResponse = objectMapper.readValue(responseBody, new TypeReference<Map<String, Object>>() {});

        } catch (JsonProcessingException e) {
            // Handle JSON parsing failures
            logger.error("Failed to parse JSON response from action '{}'. Status: {}, Body: {}", action, statusCode, responseBody, e);
            String errorMsg = String.format("Invalid JSON response from CKAN action '%s' (Status %d). Body: %s",
                    action, statusCode, responseBody.length() > 500 ? responseBody.substring(0, 500) + "..." : responseBody);
            // Distinguish connection issues (likely bad gateway returning non-JSON) from client/server errors
            if (statusCode >= 500) {
                throw new CkanConnectionException(errorMsg, e);
            } else {
                throw new CkanException(errorMsg, e); // Or potentially a validation/auth exception if status code suggests it?
            }
        }

        // Check CKAN's internal success flag
        Object successObj = ckanResponse.get("success");
        // Handle boolean true or string "true"
        boolean success = Boolean.TRUE.equals(successObj) || "true".equalsIgnoreCase(String.valueOf(successObj));

        if (success) {
            Object result = ckanResponse.get("result");
            if (result == null) {
                // Success reported, but no 'result' field where one was expected
                if (Void.class.equals(responseTypeRef.getType())) {
                    return null; // Null result is ok if expecting Void
                } else {
                    throw new CkanUnexpectedNullResultException(String.format(
                            "CKAN action '%s' succeeded but returned null 'result' when expecting type %s.",
                            action, responseTypeRef.getType()));
                }
            }
            // Convert the 'result' object to the specific type T expected by the caller
            try {
                return objectMapper.convertValue(result, responseTypeRef);
            } catch (IllegalArgumentException e) {
                // Failed conversion - likely CKAN returned a different structure than expected
                logger.error("Failed to convert CKAN 'result' for action '{}' to expected type {}. Result type was: {}. Result snippet: {}",
                        action, responseTypeRef.getType(), result.getClass().getName(),
                        String.valueOf(result).substring(0, Math.min(100, String.valueOf(result).length())), e);
                throw new CkanException("Internal error: Mismatched result structure from CKAN action '" + action +
                        "'. Expected " + responseTypeRef.getType(), e);
            }
        } else {
            // CKAN reported failure (success flag was false)
            Map<String, Object> errorDetails = null;
            Object errorObj = ckanResponse.get("error");
            if (errorObj instanceof Map) {
                errorDetails = (Map<String, Object>) errorObj;
            }

            String errorMessage = "Unknown CKAN Error";
            String errorType = "UnknownError"; // Default if __type is missing
            Map<String, Object> validationDetails = null; // Specific map for validation errors

            if (errorDetails != null) {
                errorMessage = String.valueOf(errorDetails.getOrDefault("message", errorMessage));
                errorType = String.valueOf(errorDetails.getOrDefault("__type", errorType));
                // Check if it looks like a validation error even if __type isn't explicit
                if ("Validation Error".equalsIgnoreCase(errorType) || containsValidationKeys(errorDetails)) {
                    validationDetails = errorDetails;
                    errorType = "Validation Error"; // Standardize type
                }
                logger.error("CKAN API Error for action '{}' - Type: {}, Message: {}, Details: {}",
                        action, errorType, errorMessage, errorDetails);
            } else {
                // Success:false but no 'error' object - unusual
                logger.error("CKAN API Error for action '{}' - Success flag was false, but no 'error' object found in response: {}",
                        action, responseBody);
                errorMessage = String.format("CKAN reported failure for action '%s' but did not provide error details (Status: %d)", action, statusCode);
            }

            // Map HTTP status codes and error types to specific custom exceptions
            if (statusCode == 404 || "Not Found Error".equalsIgnoreCase(errorType)) {
                throw new CkanNotFoundException(String.format("CKAN resource not found for action '%s': %s", action, errorMessage));
            } else if (statusCode == 403 || statusCode == 401 || "Authorization Error".equalsIgnoreCase(errorType)) {
                throw new CkanAuthorizationException(String.format("CKAN authorization failed for action '%s': %s", action, errorMessage));
            } else if ((statusCode == 409 || statusCode == 400) && "Validation Error".equalsIgnoreCase(errorType)) { // 409 Conflict often indicates validation issue
                throw new CkanValidationException(String.format("CKAN validation failed for action '%s'", action), validationDetails);
            } else if (statusCode >= 500) {
                // Server-side error on CKAN instance - treat as connection problem
                throw new CkanConnectionException(String.format("CKAN Server Error (Status %d) for action '%s': %s",
                        statusCode, action, errorMessage), null); // null cause as it's derived from status
            } else {
                // Catch-all for other CKAN errors reported via success:false
                throw new CkanException(String.format("CKAN API error (Type: %s, Status: %d) for action '%s': %s",
                        errorType, statusCode, action, errorMessage));
            }
        }
    }

    // Helper to identify potential validation errors even if __type is missing
    private boolean containsValidationKeys(Map<String, Object> map) {
        if (map == null) return false;
        // If map contains keys other than __type or message, assume it's validation details
        return map.keySet().stream().anyMatch(key -> !key.equals("__type") && !key.equals("message"));
    }


    // --- Public CKAN Interaction Methods ---

    /** Checks if an organization exists in CKAN. */
    public Optional<Map<String, Object>> checkOrganizationExists(String orgIdOrName) throws CkanException, IOException {
        logger.debug("Checking organization: '{}'", orgIdOrName);
        Map<String, Object> params = Map.of("id", orgIdOrName, "include_datasets", false);
        try {
            // Use TypeReference<Map<String, Object>> for generic org details
            Map<String, Object> orgDetails = sendRequest(ACTION_ORG_SHOW, params, "GET", new TypeReference<>() {});
            logger.info("Organization '{}' (ID: {}) found.", orgIdOrName, orgDetails.get("id"));
            return Optional.of(orgDetails);
        } catch (CkanNotFoundException e) {
            logger.info("Organization '{}' not found in CKAN.", orgIdOrName);
            return Optional.empty();
        }
    }

    /** Creates a new organization in CKAN. */
    public Map<String, Object> createOrganization(String orgId, String orgTitle) throws CkanException, IOException {
        logger.info("Attempting to create new organization ID='{}', Title='{}'", orgId, orgTitle);
        Map<String, Object> orgData = new HashMap<>();
        orgData.put("name", orgId); // Usually the URL slug / unique ID
        orgData.put("title", orgTitle); // Display name
        orgData.put("state", "active");
        orgData.put("description", "Organization automatically created by pipeline on " + LocalDateTime.now().format(DateTimeFormatter.ISO_DATE));
        // Use TypeReference<Map<String, Object>> for generic create response
        Map<String, Object> createdOrg = sendRequest(ACTION_ORG_CREATE, orgData, "POST", new TypeReference<>() {});
        logger.info("Organization '{}' (ID: {}) successfully created.", orgId, createdOrg.get("id"));
        return createdOrg;
    }

    /** Gets dataset details if it exists, otherwise creates it. Updates notes on existing datasets. */
    public Map<String, Object> getOrCreateDataset(String datasetId, String datasetTitle, String ownerOrgId, String sourceIdentifier) throws CkanException, IOException {
        logger.debug("Looking for or creating dataset ID/Name='{}' in org ID '{}'...", datasetId, ownerOrgId);
        try {
            // Check if dataset exists
            Map<String, Object> paramsShow = Map.of("id", datasetId);
            Map<String, Object> existingPackage = sendRequest(ACTION_PACKAGE_SHOW, paramsShow, "GET", new TypeReference<>() {});
            String packageActualId = (String) existingPackage.get("id");
            logger.info("Dataset '{}' (ID: {}) already exists.", datasetId, packageActualId);

            // Attempt to update metadata (e.g., last checked timestamp)
            try {
                String updateTimestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss 'UTC'"));
                String newNotes = String.format("Dataset content last checked/updated from source [%s] on %s.", sourceIdentifier, updateTimestamp);
                Map<String, Object> patchData = Map.of("id", packageActualId, "notes", newNotes);
                // Use package_patch for partial updates (package_update replaces all fields)
                sendRequest(ACTION_PACKAGE_PATCH, patchData, "POST", new TypeReference<Map<String, Object>>() {}); // Response often contains full package, map if needed
                logger.debug("Updated notes for dataset '{}'.", datasetId);
                // FIX 2: Use separate catch blocks for disjoint exception handling
            } catch (CkanException patchErr) { // Catch specific CKAN errors first
                logger.warn("Could not update metadata (notes) for existing dataset '{}' via CKAN API: {}. Continuing.", datasetId, patchErr.getMessage(), patchErr);
            } catch (IOException patchErr) { // Catch broader IO errors (network, etc.)
                logger.warn("Could not update metadata (notes) for existing dataset '{}' due to IO error: {}. Continuing.", datasetId, patchErr.getMessage(), patchErr);
            }
            return existingPackage;

        } catch (CkanNotFoundException e) {
            // Dataset doesn't exist, create it
            logger.info("Dataset '{}' does not exist. Attempting creation in org '{}'...", datasetId, ownerOrgId);
            Map<String, Object> packageData = new HashMap<>();
            packageData.put("name", datasetId); // URL slug / unique name
            packageData.put("title", datasetTitle); // Display title
            packageData.put("owner_org", ownerOrgId); // Owning organization ID
            packageData.put("notes", String.format("Dataset automatically created for source [%s] on %s.",
                    sourceIdentifier, LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss 'UTC'"))));
            packageData.put("author", "Data Pipeline"); // Optional metadata
            packageData.put("state", "active"); // Ensure it's visible

            Map<String, Object> createdPackage = sendRequest(ACTION_PACKAGE_CREATE, packageData, "POST", new TypeReference<>() {});
            logger.info("Dataset '{}' (ID: {}) successfully created.", datasetId, createdPackage.get("id"));
            return createdPackage;
        }
    }

    /** Fetches existing resources for a package, returning a map of resource name to resource ID. */
    public Map<String, String> getExistingResourceNamesAndIds(String packageIdOrName) throws CkanException, IOException {
        logger.debug("Fetching existing resources for package '{}'...", packageIdOrName);
        Map<String, String> existingResources = new HashMap<>();
        try {
            Map<String, Object> params = Map.of("id", packageIdOrName, "include_resources", true); // Ensure resources are included
            Map<String, Object> packageDetails = sendRequest(ACTION_PACKAGE_SHOW, params, "GET", new TypeReference<>() {});

            Object resourcesObj = packageDetails.get("resources");
            if (resourcesObj instanceof List) {
                @SuppressWarnings("unchecked") // Safe cast after instanceof check
                List<Map<String, Object>> resourcesList = (List<Map<String, Object>>) resourcesObj;
                for (Map<String, Object> res : resourcesList) {
                    String name = (String) res.get("name"); // Resource display name or filename
                    String id = (String) res.get("id");     // Resource unique ID
                    if (name != null && !name.isEmpty() && id != null) {
                        if (existingResources.containsKey(name)) {
                            // Handle duplicate names if necessary - CKAN allows them but might be confusing
                            logger.warn("Duplicate resource name '{}' found in package '{}'. Overwriting previous ID in map.", name, packageIdOrName);
                        }
                        existingResources.put(name, id);
                    } else {
                        logger.warn("Found resource with missing name or ID in package '{}'. Details: {}", packageIdOrName, res);
                    }
                }
            }
            logger.debug("Found {} named resources for package '{}'.", existingResources.size(), packageIdOrName);
            return Collections.unmodifiableMap(existingResources); // Return immutable map

        } catch (CkanNotFoundException e) {
            // If package doesn't exist, it has no resources
            logger.warn("Package '{}' not found when trying to fetch resources. Returning empty map.", packageIdOrName);
            return Collections.emptyMap();
        }
    }

    /** Uploads a file as a new resource or updates an existing resource if existingResourceId is provided. Uses streaming for large files. */
    public Map<String, Object> uploadOrUpdateResource(String packageId, Path filePath, String resourceName, String description, String format, String existingResourceId) throws CkanException, IOException {
        boolean isUpdate = (existingResourceId != null && !existingResourceId.isBlank());
        String action = isUpdate ? ACTION_RESOURCE_UPDATE : ACTION_RESOURCE_CREATE;
        String logPrefix = isUpdate ? "Updating" : "Creating";

        logger.info("{} resource '{}' for package ID '{}' from file '{}'...", logPrefix, resourceName, packageId, filePath.getFileName());

        Map<String, Object> resourceData = new HashMap<>();
        resourceData.put("package_id", packageId); // ID of the dataset this resource belongs to
        resourceData.put("name", resourceName); // Name of the resource (often filename)

        // Optional fields
        if (description != null) resourceData.put("description", description);
        if (format != null) resourceData.put("format", format.toUpperCase()); // CKAN often uses uppercase format names

        if (isUpdate) {
            resourceData.put("id", existingResourceId); // ID of the resource to update
        }

        // Field name for the file upload in the multipart request (often 'upload')
        String uploadFieldName = "upload";

        // Use the dedicated upload method which now streams
        Map<String, Object> result = sendUploadRequest(action, resourceData, filePath, uploadFieldName, new TypeReference<>() {});

        logger.info("Resource '{}' (ID: {}) successfully {}.", resourceName, result.get("id"), (isUpdate ? "updated" : "created"));
        return result; // Return the CKAN response (usually details of the created/updated resource)
    }

}
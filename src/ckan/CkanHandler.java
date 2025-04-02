// Save as: src/ckan/CkanHandler.java
package ckan; // Using package name from your snippet

// Using imports from your snippet - Recommend standardizing paths
import ckan.CkanExceptions.*;
import util.LoggingUtil; // Assuming LoggingUtil is in 'util' package

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.DeserializationFeature; // Example import if needed
// import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule; // Example import if needed

import org.slf4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException; // Import explicitly for clarity
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
 * (Javadoc remains the same...)
 */
public class CkanHandler {

    private static final Logger logger = LoggingUtil.getLogger(CkanHandler.class);
    private static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofSeconds(90);
    private static final String DEFAULT_USER_AGENT = "JavaCkanPipelineClient/1.0"; // Identify client

    // --- CKAN Action Name Constants ---
    private static final String ACTION_SITE_READ = "site_read";
    private static final String ACTION_ORG_SHOW = "organization_show";
    private static final String ACTION_ORG_CREATE = "organization_create";
    private static final String ACTION_PACKAGE_SHOW = "package_show";
    private static final String ACTION_PACKAGE_CREATE = "package_create";
    private static final String ACTION_PACKAGE_PATCH = "package_patch"; // Or package_update
    private static final String ACTION_RESOURCE_CREATE = "resource_create";
    private static final String ACTION_RESOURCE_UPDATE = "resource_update";
    // Add more action names as needed...

    private final String ckanApiUrlBase;
    private final String apiKey;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    // Constructor remains the same...
    public CkanHandler(String ckanUrl, String apiKey) {
        Objects.requireNonNull(ckanUrl, "CKAN URL cannot be null");
        if (ckanUrl.trim().isEmpty()) { throw new IllegalArgumentException("CKAN URL cannot be empty."); }
        if (!ckanUrl.trim().toLowerCase().startsWith("http")) { throw new IllegalArgumentException("CKAN URL must start with http:// or https://"); }
        String baseUrl = ckanUrl.trim();
        this.ckanApiUrlBase = (baseUrl.endsWith("/") ? baseUrl : baseUrl + "/") + "api/3/action/";
        this.apiKey = (apiKey != null) ? apiKey.trim() : "";
        if (this.apiKey.isEmpty() || "YOUR_CKAN_API_KEY_HERE".equalsIgnoreCase(this.apiKey)) { logger.warn("CKAN API Key is missing, empty, or using a placeholder value. API calls requiring authentication WILL fail."); }
        this.httpClient = HttpClient.newBuilder() .version(HttpClient.Version.HTTP_1_1) .connectTimeout(DEFAULT_REQUEST_TIMEOUT) .followRedirects(HttpClient.Redirect.NORMAL) .build();
        this.objectMapper = new ObjectMapper();
        logger.info("CKAN Handler initialized. API Base: {}", this.ckanApiUrlBase);
    }

    // testConnection method remains the same ...
    public void testConnection() throws CkanException, IOException {
        logger.info("Attempting test connection to CKAN (site_read)..."); try { sendRequest(ACTION_SITE_READ, null, "GET", new TypeReference<Map<String, Object>>() {}); logger.info("Test connection successful."); } catch (CkanException e) { logger.error("Test connection failed: {}", e.getMessage()); throw e; }
    }

    // --- Internal Request Sending Logic ---
    // sendRequest overloads remain the same ...
    private <T> T sendRequest(String action, String method, TypeReference<T> responseTypeRef) throws CkanException, IOException { return sendRequestInternal(action, null, method, responseTypeRef, null, null); }
    private <T> T sendRequest(String action, Map<String, Object> data, String method, TypeReference<T> responseTypeRef) throws CkanException, IOException { return sendRequestInternal(action, data, method, responseTypeRef, null, null); }
    private <T> T sendUploadRequest(String action, Map<String, Object> data, Path fileToUpload, String uploadFieldName, TypeReference<T> responseTypeRef) throws CkanException, IOException { Objects.requireNonNull(fileToUpload, "File to upload cannot be null."); Objects.requireNonNull(uploadFieldName, "Upload field name cannot be null."); if (!Files.isRegularFile(fileToUpload) || !Files.isReadable(fileToUpload)) { throw new IOException("File to upload must be a readable regular file: " + fileToUpload); } if (uploadFieldName.isBlank()) { throw new IllegalArgumentException("Upload field name cannot be blank."); } return sendRequestInternal(action, data, "POST", responseTypeRef, fileToUpload, uploadFieldName); }

    // sendRequestInternal method remains the same ...
    private <T> T sendRequestInternal(String action, Map<String, Object> data, String method, TypeReference<T> responseTypeRef, Path fileToUpload, String uploadFieldName) throws CkanException, IOException {
        URI uri = URI.create(ckanApiUrlBase + action); HttpRequest.Builder requestBuilder = HttpRequest.newBuilder() .uri(uri) .timeout(DEFAULT_REQUEST_TIMEOUT) .header("User-Agent", DEFAULT_USER_AGENT); if (this.apiKey != null && !this.apiKey.isEmpty()) { requestBuilder.header("Authorization", this.apiKey); } else { logger.debug("Sending request to {} without Authorization header (API key not provided).", action); } HttpRequest request; long requestStartTime = System.nanoTime(); try { if ("POST".equalsIgnoreCase(method)) { if (fileToUpload != null) { String boundary = "Boundary-" + UUID.randomUUID().toString(); requestBuilder.header("Content-Type", "multipart/form-data; boundary=" + boundary); MimeMultipartData bodyData = MimeMultipartData.newBuilder(boundary) .addTextParts(data) .addFilePart(uploadFieldName, fileToUpload) .build(); request = requestBuilder.POST(bodyData.getBodyPublisher()).build(); logger.debug("POST (Multipart) Request to: {} with fields: {} and file: {}", uri, data != null ? data.keySet() : "none", fileToUpload.getFileName()); } else { requestBuilder.header("Content-Type", "application/json; charset=utf-8"); String requestBody = (data != null && !data.isEmpty()) ? objectMapper.writeValueAsString(data) : "{}"; logger.trace("POST Request to: {} with body: {}", uri, requestBody); request = requestBuilder.POST(HttpRequest.BodyPublishers.ofString(requestBody, StandardCharsets.UTF_8)).build(); } } else { URIBuilder uriBuilder = new URIBuilder(uri); if (data != null && !data.isEmpty()) { data.forEach((key, value) -> { if (value != null) { uriBuilder.addParameter(key, String.valueOf(value)); } }); } try { uri = uriBuilder.build(); } catch (URISyntaxException e) { throw new CkanException("Internal error: Failed to build GET URI for " + action, e); } requestBuilder.uri(uri); logger.debug("GET Request to: {}", uri); request = requestBuilder.GET().build(); } HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8)); long durationMillis = Duration.ofNanos(System.nanoTime() - requestStartTime).toMillis(); logger.info("CKAN call '{}' completed with status {} in {} ms", action, response.statusCode(), durationMillis); return handleResponse(response, action, responseTypeRef); } catch (JsonProcessingException e) { throw new CkanException("Internal error: Failed to create JSON request for " + action, e); } catch (IOException e) { logger.error("Connection or I/O error during CKAN request for action {}: {}", action, e.getMessage(), e); Throwable cause = e.getCause(); if (cause instanceof InterruptedException) { Thread.currentThread().interrupt(); logger.warn("CKAN request thread interrupted for action: {}", action); } throw new CkanConnectionException("Communication or I/O error with CKAN for action " + action + ": " + e.getMessage(), e); } catch (InterruptedException e) { Thread.currentThread().interrupt(); logger.warn("CKAN request thread interrupted for action: {}", action); throw new CkanConnectionException("CKAN request interrupted for action " + action, e); }
    }

    // URIBuilder class remains the same...
    private static class URIBuilder { private final URI baseUri; private final List<Map.Entry<String, String>> params = new ArrayList<>(); URIBuilder(URI baseUri) { this.baseUri = baseUri; } URIBuilder addParameter(String key, String value) { params.add(Map.entry(key, value)); return this; } URI build() throws URISyntaxException { if (params.isEmpty()) return baseUri; String q=baseUri.getRawQuery(); StringBuilder s=new StringBuilder(q==null?"":q); if(s.length()>0&&!params.isEmpty())s.append('&'); s.append(params.stream().map(e->URLEncoder.encode(e.getKey(),StandardCharsets.UTF_8)+"="+URLEncoder.encode(e.getValue(),StandardCharsets.UTF_8)).collect(Collectors.joining("&"))); return new URI(baseUri.getScheme(),baseUri.getAuthority(),baseUri.getPath(),s.toString(),baseUri.getFragment()); } }

    // handleResponse method - Includes previous fix for CkanConnectionException call
    @SuppressWarnings("unchecked")
    private <T> T handleResponse(HttpResponse<String> response, String action, TypeReference<T> responseTypeRef) throws CkanException {
        int statusCode = response.statusCode(); String responseBody = response.body();
        logger.debug("Response from action '{}' ({}) Headers: {}", action, statusCode, response.headers().map()); if (logger.isTraceEnabled()) { logger.trace("Response from action '{}' ({}) Body: {}", action, statusCode, responseBody); } else if (responseBody != null && !responseBody.isEmpty()) { logger.debug("Response from action '{}' ({}) Body: {}", action, statusCode, responseBody.length() > 1024 ? responseBody.substring(0, 1024) + "... (truncated)" : responseBody); } Map<String, Object> ckanResponse; try { if (responseBody == null || responseBody.isBlank()) { if (statusCode >= 200 && statusCode < 300) { if (responseTypeRef.getType().equals(Void.class)) { return null; } else { throw new CkanException(String.format("CKAN action '%s' succeeded (%d) but returned an empty response body when expecting type %s.", action, statusCode, responseTypeRef.getType())); } } else { throw new CkanException(String.format("CKAN action '%s' failed (%d) and returned an empty response body.", action, statusCode)); } } ckanResponse = objectMapper.readValue(responseBody, new TypeReference<Map<String, Object>>() {}); } catch (JsonProcessingException e) { logger.error("Failed to parse JSON response from action '{}'. Status: {}, Body: {}", action, statusCode, responseBody, e); String errorMsg = String.format("Invalid JSON response from CKAN action '%s' (Status %d). Body: %s", action, statusCode, responseBody); if (statusCode >= 500) { throw new CkanConnectionException(errorMsg, e); } else { throw new CkanException(errorMsg, e); } } Object successObj = ckanResponse.get("success"); boolean success = Boolean.TRUE.equals(successObj) || "true".equalsIgnoreCase(String.valueOf(successObj)); if (success) { Object result = ckanResponse.get("result"); if (result == null) { if (responseTypeRef.getType().equals(Void.class)) { return null; } else { throw new CkanUnexpectedNullResultException(String.format( "CKAN action '%s' succeeded but returned null 'result' when expecting type %s.", action, responseTypeRef.getType())); } } try { return objectMapper.convertValue(result, responseTypeRef); } catch (IllegalArgumentException e) { logger.error("Failed to convert CKAN 'result' for action '{}' to expected type {}. Result type was: {}. Result snippet: {}", action, responseTypeRef.getType(), result.getClass().getName(), String.valueOf(result).substring(0, Math.min(100, String.valueOf(result).length())), e); throw new CkanException("Internal error: Mismatched result structure from CKAN action '" + action + "'. Expected " + responseTypeRef.getType(), e); }
        } else { Map<String, Object> errorDetails = null; Object errorObj = ckanResponse.get("error"); if (errorObj instanceof Map) { errorDetails = (Map<String, Object>) errorObj; } String errorMessage = "Unknown CKAN Error"; String errorType = "UnknownError"; Map<String, Object> validationDetails = null; if (errorDetails != null) { errorMessage = String.valueOf(errorDetails.getOrDefault("message", errorMessage)); errorType = String.valueOf(errorDetails.getOrDefault("__type", errorType)); if ("Validation Error".equalsIgnoreCase(errorType) || containsValidationKeys(errorDetails)) { validationDetails = errorDetails; errorType = "Validation Error"; } logger.error("CKAN API Error for action '{}' - Type: {}, Message: {}, Details: {}", action, errorType, errorMessage, errorDetails); } else { logger.error("CKAN API Error for action '{}' - Success flag was false, but no 'error' object found in response: {}", action, responseBody); errorMessage = String.format("CKAN reported failure for action '%s' but did not provide error details (Status: %d)", action, statusCode); }
            if (statusCode == 404 || "Not Found Error".equalsIgnoreCase(errorType)) { throw new CkanNotFoundException(String.format("CKAN resource not found for action '%s': %s", action, errorMessage)); } else if (statusCode == 403 || statusCode == 401 || "Authorization Error".equalsIgnoreCase(errorType)) { throw new CkanAuthorizationException(String.format("CKAN authorization failed for action '%s': %s", action, errorMessage)); } else if ((statusCode == 409 || statusCode == 400) && "Validation Error".equalsIgnoreCase(errorType)) { throw new CkanValidationException(String.format("CKAN validation failed for action '%s'", action), validationDetails); } else if (statusCode >= 500) {
                // This call requires CkanConnectionException(String, Throwable)
                // Passing 'null' for cause is now allowed by the modified CkanExceptions.java
                throw new CkanConnectionException(String.format("CKAN Server Error (Status %d) for action '%s': %s", statusCode, action, errorMessage), null);
            } else { throw new CkanException(String.format("CKAN API error (Type: %s, Status: %d) for action '%s': %s", errorType, statusCode, action, errorMessage)); }
        }
    }

    // containsValidationKeys method remains the same...
    private boolean containsValidationKeys(Map<String, Object> map) { if (map == null) return false; return map.keySet().stream().anyMatch(key -> !key.equals("__type") && !key.equals("message")); }

    // --- Public CKAN Interaction Methods ---
    // (Methods remain the same, including throws declarations and previous fixes)
    public Optional<Map<String, Object>> checkOrganizationExists(String orgIdOrName) throws CkanException, IOException { logger.debug("Checking organization: '{}'", orgIdOrName); Map<String, Object> params = Map.of("id", orgIdOrName, "include_datasets", false); try { Map<String, Object> orgDetails = sendRequest(ACTION_ORG_SHOW, params, "GET", new TypeReference<>() {}); logger.info("Organization '{}' (ID: {}) found.", orgIdOrName, orgDetails.get("id")); return Optional.of(orgDetails); } catch (CkanNotFoundException e) { logger.info("Organization '{}' not found in CKAN.", orgIdOrName); return Optional.empty(); } }
    public Map<String, Object> createOrganization(String orgId, String orgTitle) throws CkanException, IOException { logger.info("Attempting to create new organization ID='{}', Title='{}'", orgId, orgTitle); Map<String, Object> orgData = new HashMap<>(); orgData.put("name", orgId); orgData.put("title", orgTitle); orgData.put("state", "active"); orgData.put("description", "Organization automatically created by pipeline on " + LocalDateTime.now().format(DateTimeFormatter.ISO_DATE)); Map<String, Object> createdOrg = sendRequest(ACTION_ORG_CREATE, orgData, "POST", new TypeReference<>() {}); logger.info("Organization '{}' (ID: {}) successfully created.", orgId, createdOrg.get("id")); return createdOrg; }
    public Map<String, Object> getOrCreateDataset(String datasetId, String datasetTitle, String ownerOrgId, String sourceIdentifier) throws CkanException, IOException { logger.debug("Looking for or creating dataset ID/Name='{}' in org ID '{}'...", datasetId, ownerOrgId); try { Map<String, Object> paramsShow = Map.of("id", datasetId); Map<String, Object> existingPackage = sendRequest(ACTION_PACKAGE_SHOW, paramsShow, "GET", new TypeReference<>() {}); String packageActualId = (String) existingPackage.get("id"); logger.info("Dataset '{}' (ID: {}) already exists.", datasetId, packageActualId); try { String updateTimestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss 'UTC'")); String newNotes = String.format("Dataset content last checked/updated from source [%s] on %s.", sourceIdentifier, updateTimestamp); Map<String, Object> patchData = Map.of("id", packageActualId, "notes", newNotes); sendRequest(ACTION_PACKAGE_PATCH, patchData, "POST", new TypeReference<Map<String, Object>>() {}); logger.debug("Updated notes for dataset '{}'.", datasetId); } catch (IOException patchErr) { logger.warn("Could not update metadata (notes) for existing dataset '{}': {}. Continuing.", datasetId, patchErr.getMessage(), patchErr); } return existingPackage; } catch (CkanNotFoundException e) { logger.info("Dataset '{}' does not exist. Attempting creation in org '{}'...", datasetId, ownerOrgId); Map<String, Object> packageData = new HashMap<>(); packageData.put("name", datasetId); packageData.put("title", datasetTitle); packageData.put("owner_org", ownerOrgId); packageData.put("notes", String.format("Dataset automatically created for source [%s] on %s.", sourceIdentifier, LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss 'UTC'")))); packageData.put("author", "Data Pipeline"); packageData.put("state", "active"); Map<String, Object> createdPackage = sendRequest(ACTION_PACKAGE_CREATE, packageData, "POST", new TypeReference<>() {}); logger.info("Dataset '{}' (ID: {}) successfully created.", datasetId, createdPackage.get("id")); return createdPackage; } }
    public Map<String, String> getExistingResourceNamesAndIds(String packageIdOrName) throws CkanException, IOException { logger.debug("Fetching existing resources for package '{}'...", packageIdOrName); Map<String, String> existingResources = new HashMap<>(); try { Map<String, Object> params = Map.of("id", packageIdOrName, "include_resources", true); Map<String, Object> packageDetails = sendRequest(ACTION_PACKAGE_SHOW, params, "GET", new TypeReference<>() {}); Object resourcesObj = packageDetails.get("resources"); if (resourcesObj instanceof List) { @SuppressWarnings("unchecked") List<Map<String, Object>> resourcesList = (List<Map<String, Object>>) resourcesObj; for (Map<String, Object> res : resourcesList) { String name = (String) res.get("name"); String id = (String) res.get("id"); if (name != null && !name.isEmpty() && id != null) { if (existingResources.containsKey(name)) { logger.warn("Duplicate resource name '{}' found in package '{}'. Overwriting previous ID.", name, packageIdOrName); } existingResources.put(name, id); } else { logger.warn("Found resource with missing name or ID in package '{}'. Details: {}", packageIdOrName, res); } } } logger.debug("Found {} named resources for package '{}'.", existingResources.size(), packageIdOrName); return Collections.unmodifiableMap(existingResources); } catch (CkanNotFoundException e) { logger.warn("Package '{}' not found when trying to fetch resources. Returning empty map.", packageIdOrName); return Collections.emptyMap(); } }
    public Map<String, Object> uploadOrUpdateResource(String packageId, Path filePath, String resourceName, String description, String format, String existingResourceId) throws CkanException, IOException { boolean isUpdate = (existingResourceId != null && !existingResourceId.isBlank()); String action = isUpdate ? ACTION_RESOURCE_UPDATE : ACTION_RESOURCE_CREATE; String logPrefix = isUpdate ? "Updating" : "Creating"; logger.info("{} resource '{}' for package ID '{}' from file '{}'...", logPrefix, resourceName, packageId, filePath.getFileName()); Map<String, Object> resourceData = new HashMap<>(); resourceData.put("package_id", packageId); resourceData.put("name", resourceName); if (description != null) resourceData.put("description", description); if (format != null) resourceData.put("format", format.toUpperCase()); if (isUpdate) { resourceData.put("id", existingResourceId); } String uploadFieldName = "upload"; Map<String, Object> result = sendUploadRequest(action, resourceData, filePath, uploadFieldName, new TypeReference<>() {}); logger.info("Resource '{}' (ID: {}) successfully {}.", resourceName, result.get("id"), (isUpdate ? "updated" : "created")); return result; }

    // --- Helper for Multipart Body Construction ---
    // (MimeMultipartData class remains the same, including build() fix)
    private static class MimeMultipartData { private final String boundary; private final List<byte[]> byteArrays = new ArrayList<>(); private static final String CRLF = "\r\n"; private MimeMultipartData(String boundary) { this.boundary = boundary; } public static Builder newBuilder(String boundary) { return new Builder(boundary); } public HttpRequest.BodyPublisher getBodyPublisher() { byteArrays.add(("--" + boundary + "--" + CRLF).getBytes(StandardCharsets.UTF_8)); return HttpRequest.BodyPublishers.ofByteArrays(byteArrays); } public static class Builder { private final MimeMultipartData instance; Builder(String boundary) { this.instance = new MimeMultipartData(boundary); } public Builder addTextPart(String name, String value) { Objects.requireNonNull(name); Objects.requireNonNull(value); String part = "--" + instance.boundary + CRLF + "Content-Disposition: form-data; name=\"" + name + "\"" + CRLF + "Content-Type: text/plain; charset=UTF-8" + CRLF + CRLF + value + CRLF; instance.byteArrays.add(part.getBytes(StandardCharsets.UTF_8)); return this; } public Builder addTextParts(Map<String, Object> parts) { if (parts != null) { parts.forEach((key, value) -> { if (value != null) { addTextPart(key, String.valueOf(value)); } }); } return this; } public Builder addFilePart(String name, Path file) throws IOException { Objects.requireNonNull(name); Objects.requireNonNull(file); if (!Files.isReadable(file)) { throw new IOException("File is not readable: " + file); } String fileName = file.getFileName().toString(); String mimeType = Files.probeContentType(file); mimeType = (mimeType == null) ? "application/octet-stream" : mimeType; String partHeader = "--" + instance.boundary + CRLF + "Content-Disposition: form-data; name=\"" + name + "\"; filename=\"" + fileName + "\"" + CRLF + "Content-Type: " + mimeType + CRLF + CRLF; instance.byteArrays.add(partHeader.getBytes(StandardCharsets.UTF_8)); instance.byteArrays.add(Files.readAllBytes(file)); instance.byteArrays.add(CRLF.getBytes(StandardCharsets.UTF_8)); return this; } public MimeMultipartData build() { if (instance.byteArrays.isEmpty()) { throw new IllegalStateException("Cannot build multipart data with no parts added."); } return instance; } } }

    // --- Custom Exception for Unexpected Null Results ---
    // (CkanUnexpectedNullResultException class remains the same)
    public static final class CkanUnexpectedNullResultException extends CkanException { public CkanUnexpectedNullResultException(String message) { super(message); } }
}
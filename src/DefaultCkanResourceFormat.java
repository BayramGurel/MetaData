import org.apache.tika.language.detect.LanguageDetector;
import org.apache.tika.language.detect.LanguageResult;
import org.apache.tika.metadata.DublinCore;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.Property;
import org.apache.tika.metadata.TikaCoreProperties;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*; // Includes List, Map, Optional, Arrays, Objects
import java.util.regex.Pattern;

/**
 * Standard implementation of {@link ICkanResourceFormatter}.
 * Formats Tika output into a {@link CkanResource}.
 * Corrected based on "old code" logic and using DublinCore.MODIFIED.
 */
public class DefaultCkanResourceFormat implements ICkanResourceFormatter {

    private final LanguageDetector languageDetector; // Can be null
    private final ExtractorConfiguration config;

    /** Constructor. */
    public DefaultCkanResourceFormat(LanguageDetector languageDetector, ExtractorConfiguration config) {
        this.languageDetector = languageDetector;
        this.config = Objects.requireNonNull(config, "Configuration cannot be null");
        if (this.languageDetector == null) {
            System.err.println("Warning: No LanguageDetector provided. Language detection will be skipped.");
        }
    }

    /** Formats the input into a CkanResource. */
    @Override
    public CkanResource format(String entryName, Metadata metadata, String text, String sourceIdentifier) {
        Map<String, Object> resourceData = new LinkedHashMap<>();
        Map<String, String> extras = new LinkedHashMap<>();
        String filename = AbstractSourceProcessor.getFilenameFromEntry(entryName); // Use static helper

        populateCoreFields(resourceData, metadata, text, filename);
        populateDates(resourceData, metadata); // Call updated populateDates
        populateExtrasMap(extras, metadata, text, sourceIdentifier, filename, entryName);

        if (!extras.isEmpty()) {
            resourceData.put("extras", extras);
        }
        return new CkanResource(resourceData);
    }

    // --- Private Helper Methods ---

    /** Populates the core CKAN resource fields. */
    private void populateCoreFields(Map<String, Object> resourceData, Metadata metadata, String text, String filename) {
        resourceData.put("package_id", "PLACEHOLDER_PACKAGE_ID");
        resourceData.put("url", config.getPlaceholderUri());

        String originalTitle = getMetadataValue(metadata, TikaCoreProperties.TITLE);
        String cleanedTitle = cleanTitle(originalTitle, config.getTitlePrefixPattern());
        String resourceName = Optional.ofNullable(cleanedTitle)
                .or(() -> Optional.ofNullable(originalTitle))
                .orElse(filename);
        resourceData.put("name", resourceName);

        String description = generateDescription(metadata, text, filename);
        resourceData.put("description", description);

        String contentType = getMetadataValue(metadata, Metadata.CONTENT_TYPE);
        resourceData.put("format", mapContentTypeToSimpleFormat(contentType));
        resourceData.put("mimetype", Optional.ofNullable(contentType)
                .map(ct -> ct.split(";")[0].trim())
                .filter(mt -> !mt.isBlank())
                .orElse(null));
    }

    /** Populates date fields (created, last_modified) if available. */
    private void populateDates(Map<String, Object> resourceData, Metadata metadata) {
        // Created date
        parseToInstant(metadata.get(TikaCoreProperties.CREATED))
                .flatMap(instant -> formatInstantToIso8601(instant, config.getIso8601Formatter()))
                .ifPresent(isoDate -> resourceData.put("created", isoDate));

        // Modified date: Try TikaCoreProperties.MODIFIED first, then DublinCore.MODIFIED
        String modifiedDateString = Optional.ofNullable(metadata.get(TikaCoreProperties.MODIFIED))
                .orElse(metadata.get(DublinCore.MODIFIED)); // Use DublinCore.MODIFIED as fallback
        parseToInstant(modifiedDateString)
                .flatMap(instant -> formatInstantToIso8601(instant, config.getIso8601Formatter()))
                .ifPresent(isoDate -> resourceData.put("last_modified", isoDate));
    }

    /** Populates the 'extras' map with additional metadata. */
    private void populateExtrasMap(Map<String, String> extras, Metadata metadata, String text,
                                   String sourceIdentifier, String filename, String originalEntryName) {
        addExtraIfPresent(extras, "source_identifier", sourceIdentifier);
        addExtraIfPresent(extras, "original_entry_name", originalEntryName);
        addExtraIfPresent(extras, "original_filename", filename);
        addExtraIfPresent(extras, "creator", getMetadataValue(metadata, TikaCoreProperties.CREATOR));

        // Language detection
        detectLanguage(text, config.getMaxTextSampleLength())
                .filter(LanguageResult::isReasonablyCertain)
                .ifPresentOrElse(
                        langResult -> addExtraIfPresent(extras, "language_uri", mapLanguageCodeToNalUri(langResult.getLanguage())),
                        () -> addExtraIfPresent(extras, "language_uri", mapLanguageCodeToNalUri("und"))
                );
    }

    /** Adds key-value to map only if value is not null or blank. */
    private static void addExtraIfPresent(Map<String, String> extrasMap, String key, String value) {
        if (value != null && !value.trim().isEmpty()) {
            extrasMap.put(key, value.trim());
        }
    }

    /** Cleans a title by removing common prefixes and normalizing whitespace. */
    private static String cleanTitle(String title, Pattern prefixPattern) {
        if (title == null || title.trim().isEmpty()) return null;
        String cleaned = prefixPattern.matcher(title).replaceFirst("");
        cleaned = cleaned.replace('_', ' ');
        cleaned = cleaned.replaceAll("\\s+", " ").trim();
        return cleaned.isEmpty() ? null : cleaned;
    }

    /** Safely gets a metadata value using a Tika Property object. */
    private static String getMetadataValue(Metadata metadata, Property property) {
        String value = metadata.get(property);
        return (value != null && !value.trim().isEmpty()) ? value.trim() : null;
    }

    /** Safely gets a metadata value using a String key. */
    private static String getMetadataValue(Metadata metadata, String key) {
        String value = metadata.get(key);
        return (value != null && !value.trim().isEmpty()) ? value.trim() : null;
    }

    /** Formats an Instant to an ISO 8601 string, returning Optional. */
    private static Optional<String> formatInstantToIso8601(Instant instant, DateTimeFormatter formatter) {
        return Optional.ofNullable(instant).map(formatter::format);
    }

    /** Attempts to parse various date/time string formats into an Instant. */
    private static Optional<Instant> parseToInstant(String dateTimeString) {
        if (dateTimeString == null || dateTimeString.trim().isEmpty()) return Optional.empty();
        List<DateTimeFormatter> formatters = Arrays.asList(
                DateTimeFormatter.ISO_OFFSET_DATE_TIME, DateTimeFormatter.ISO_ZONED_DATE_TIME,
                DateTimeFormatter.ISO_INSTANT, DateTimeFormatter.ISO_LOCAL_DATE_TIME,
                DateTimeFormatter.ISO_LOCAL_DATE);

        for (DateTimeFormatter formatter : formatters) {
            try {
                if (formatter == DateTimeFormatter.ISO_OFFSET_DATE_TIME)
                    return Optional.of(OffsetDateTime.parse(dateTimeString, formatter).toInstant());
                if (formatter == DateTimeFormatter.ISO_ZONED_DATE_TIME)
                    return Optional.of(ZonedDateTime.parse(dateTimeString, formatter).toInstant());
                if (formatter == DateTimeFormatter.ISO_INSTANT)
                    return Optional.of(Instant.parse(dateTimeString));
                if (formatter == DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                    return Optional.of(LocalDateTime.parse(dateTimeString, formatter).atZone(ZoneId.systemDefault()).toInstant());
                if (formatter == DateTimeFormatter.ISO_LOCAL_DATE)
                    return Optional.of(LocalDate.parse(dateTimeString, formatter).atStartOfDay(ZoneId.systemDefault()).toInstant());
            } catch (DateTimeParseException ignored) {}
        }
        System.err.println("Warning: Could not parse date/time string: " + dateTimeString);
        return Optional.empty();
    }

    /** Maps a Tika content type to a simple format string (e.g., PDF, DOCX). */
    private static String mapContentTypeToSimpleFormat(String contentType) {
        return Optional.ofNullable(contentType)
                .filter(ct -> !ct.isBlank())
                .map(ct -> ct.toLowerCase().split(";")[0].trim())
                .map(lowerType -> switch (lowerType) {
                    // Cases remain the same...
                    case "application/pdf" -> "PDF";
                    case "application/vnd.openxmlformats-officedocument.wordprocessingml.document" -> "DOCX";
                    case "application/msword" -> "DOC";
                    case "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" -> "XLSX";
                    case "application/vnd.ms-excel" -> "XLS";
                    case "application/vnd.openxmlformats-officedocument.presentationml.presentation" -> "PPTX";
                    case "application/vnd.ms-powerpoint" -> "PPT";
                    case "text/plain" -> "TXT";
                    case "text/csv" -> "CSV";
                    case "application/xml", "text/xml" -> "XML";
                    case "text/html" -> "HTML";
                    case "image/jpeg", "image/jpg" -> "JPEG";
                    case "image/png" -> "PNG";
                    case "image/gif" -> "GIF";
                    case "image/tiff" -> "TIFF";
                    case "application/zip" -> "ZIP";
                    case "application/shp", "application/x-shapefile", "application/vnd.shp" -> "SHP";
                    case "application/dbf", "application/x-dbf", "application/vnd.dbf" -> "DBF";
                    case "application/vnd.google-earth.kml+xml" -> "KML";
                    case "application/vnd.google-earth.kmz" -> "KMZ";
                    case "application/geo+json", "application/vnd.geo+json" -> "GeoJSON";
                    case "application/geopackage+sqlite3", "application/x-sqlite3" -> "GPKG";
                    default -> {
                        int lastSlash = lowerType.lastIndexOf('/');
                        String format = (lastSlash != -1 && lastSlash < lowerType.length() - 1)
                                ? lowerType.substring(lastSlash + 1) : lowerType;
                        yield format.toUpperCase().replaceAll("[^A-Z0-9]", "");
                    }
                })
                .orElse("Unknown");
    }

    /** Maps an ISO 639-1 language code (e.g., "nl") to a NAL URI. */
    private static String mapLanguageCodeToNalUri(String languageCode) {
        final String NAL_BASE_URI = "http://publications.europa.eu/resource/authority/language/";
        if (languageCode == null || languageCode.isBlank() || languageCode.equalsIgnoreCase("und")) {
            return NAL_BASE_URI + "UND";
        }
        String baseCode = languageCode.toLowerCase().split("-")[0];
        return NAL_BASE_URI + switch (baseCode) {
            case "nl" -> "NLD"; case "en" -> "ENG"; case "de" -> "DEU";
            case "fr" -> "FRA"; case "es" -> "SPA"; case "it" -> "ITA";
            default -> "MUL";
        };
    }

    /**
     * Generates a description, preferring metadata fields, falling back to a text snippet.
     * Logic restored closer to the "old code".
     */
    private String generateDescription(Metadata metadata, String text, String filename) {
        String description = getMetadataValue(metadata, DublinCore.DESCRIPTION);
        if (!isDescriptionValid(description)) {
            description = getMetadataValue(metadata, TikaCoreProperties.DESCRIPTION);
            if (!isDescriptionValid(description)) {
                description = null;
            }
        }

        if (description != null) {
            String cleanedDesc = description.trim().replaceAll("\\s+", " ");
            if (cleanedDesc.length() > config.getMaxAutoDescriptionLength()) {
                int end = config.getMaxAutoDescriptionLength();
                int lastSpace = cleanedDesc.lastIndexOf(' ', end - 3);
                if (lastSpace > end / 2) {
                    cleanedDesc = cleanedDesc.substring(0, lastSpace).trim() + "...";
                } else {
                    cleanedDesc = cleanedDesc.substring(0, end - 3).trim() + "...";
                }
            }
            cleanedDesc = cleanedDesc.replaceAll("^[\\W_]+|[\\W_]+$", "").trim();
            if (!cleanedDesc.isEmpty()) {
                return cleanedDesc;
            }
        }

        if (text != null && !text.isBlank()) {
            String cleanedText = text.trim().replaceAll("\\s+", " ");
            if (cleanedText.length() > 10) {
                int endIndex = Math.min(cleanedText.length(), config.getMaxAutoDescriptionLength());
                String snippet = cleanedText.substring(0, endIndex).trim();
                snippet = snippet.replaceAll("^[\\W_]+|[\\W_]+$", "").trim();
                String suffix = (cleanedText.length() > endIndex && snippet.length() < config.getMaxAutoDescriptionLength()) ? "..." : "";
                if (!snippet.isEmpty()) {
                    return snippet + suffix;
                }
            }
        }

        return "Geen beschrijving beschikbaar voor: " + filename;
    }

    /**
     * Checks if a description string meets basic quality criteria.
     * Uses the 'config' field directly, like in the old code.
     */
    private boolean isDescriptionValid(String description) {
        if (description == null || description.isBlank()) return false;
        String trimmed = description.trim();
        return trimmed.length() >= this.config.getMinDescMetadataLength()
                && trimmed.length() <= this.config.getMaxDescMetadataLength()
                && !trimmed.contains("_x000d_")
                && !trimmed.equalsIgnoreCase("untitled")
                && !trimmed.equalsIgnoreCase("no title")
                && !trimmed.matches("(?i)^\\s*template\\s*$");
    }

    /** Cleans a description: trims, normalizes whitespace, truncates gracefully. */
    private static String cleanDescription(String description, int maxLength) {
        if (description == null) return null;
        String cleaned = description.trim().replaceAll("\\s+", " ");
        if (cleaned.length() > maxLength) {
            int lastSpace = cleaned.lastIndexOf(' ', maxLength - 3);
            cleaned = (lastSpace > maxLength / 2)
                    ? cleaned.substring(0, lastSpace).trim() + "..."
                    : cleaned.substring(0, maxLength - 3).trim() + "...";
        }
        return cleaned.replaceAll("^[\\W_]+|[\\W_]+$", "").trim();
    }

    /** Detects language using Tika, returning Optional<LanguageResult>. */
    private Optional<LanguageResult> detectLanguage(String text, int maxSampleLength) {
        if (this.languageDetector == null || text == null || text.trim().isEmpty()) {
            return Optional.empty();
        }
        try {
            String sample = text.substring(0, Math.min(text.length(), maxSampleLength)).trim();
            if (sample.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(this.languageDetector.detect(sample));
        } catch (Exception e) {
            System.err.println("Warning: Language detection failed: " + e.getMessage());
            return Optional.empty();
        }
    }
}

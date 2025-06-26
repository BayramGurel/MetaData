import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.tika.langdetect.optimaize.OptimaizeLangDetector;
import org.apache.tika.language.detect.LanguageDetector;

public class MetadataExtractor {
    private static final String SOURCE_ZIP = ".\\document\\Veg kartering - habitatkaart 2021-2023.zip";
    private static final String PACKAGE_ID = "zuid-holland-habitatkaart-2021";
    private final IFileTypeFilter fileFilter;
    private final IMetadataProvider metadataProvider;
    private final ICkanResourceFormatter resourceFormatter;
    private final ExtractorConfiguration config;

    public MetadataExtractor(IFileTypeFilter fileFilter, IMetadataProvider metadataProvider, ICkanResourceFormatter resourceFormatter, ExtractorConfiguration config) {
        this.fileFilter = (IFileTypeFilter)Objects.requireNonNull(fileFilter, "File filter mag niet null zijn");
        this.metadataProvider = (IMetadataProvider)Objects.requireNonNull(metadataProvider, "Metadata provider mag niet null zijn");
        this.resourceFormatter = (ICkanResourceFormatter)Objects.requireNonNull(resourceFormatter, "Resource formatter mag niet null zijn");
        this.config = (ExtractorConfiguration)Objects.requireNonNull(config, "Configuratie mag niet null zijn");
    }

    public ProcessingReport processSource(String sourcePathString) {
        List<CkanResource> results = new ArrayList();
        List<ProcessingError> errors = new ArrayList();
        List<IgnoredEntry> ignored = new ArrayList();
        Path sourcePath = null;

        try {
            sourcePath = Paths.get(sourcePathString).toAbsolutePath().normalize();
            if (!Files.exists(sourcePath, new LinkOption[0])) {
                errors.add(new ProcessingError(sourcePathString, "Bron niet gevonden."));
                return new ProcessingReport(results, errors, ignored);
            }

            if (Files.isDirectory(sourcePath, new LinkOption[0])) {
                ignored.add(new IgnoredEntry(sourcePathString, "Bron is map (niet ondersteund)."));
                return new ProcessingReport(results, errors, ignored);
            }

            ISourceProcessor processor = (ISourceProcessor)(this.isZipFile(sourcePath) ? new ZipSourceProcessor(this.fileFilter, this.metadataProvider, this.resourceFormatter, this.config) : new SingleFileProcessor(this.fileFilter, this.metadataProvider, this.resourceFormatter, this.config));
            processor.processSource(sourcePath, sourcePath.toString(), results, errors, ignored);
        } catch (Exception e) {
            String pathForError = sourcePath != null ? sourcePath.toString() : sourcePathString;
            errors.add(new ProcessingError(pathForError, "Kritieke fout: " + e.getMessage()));
            e.printStackTrace(System.err);
        }

        System.err.printf("Samenvatting: %d resources, %d fouten, %d genegeerd%n", results.size(), errors.size(), ignored.size());
        return new ProcessingReport(results, errors, ignored);
    }

    private boolean isZipFile(Path path) {
        if (path != null && Files.isRegularFile(path, new LinkOption[0])) {
            String name = path.getFileName().toString().toLowerCase();
            return this.config.getSupportedZipExtensions().stream().anyMatch((ext) -> name.endsWith(ext.toLowerCase()));
        } else {
            return false;
        }
    }

    public static void main(String[] args) {
        String inputPath = args.length > 0 ? args[0] : SOURCE_ZIP;
        Path src = Paths.get(inputPath).toAbsolutePath().normalize();
        if (!Files.exists(src, new LinkOption[0])) {
            System.err.println("FATAL: Bron niet gevonden: " + src);
            System.exit(1);
        }

        ExtractorConfiguration config = new ExtractorConfiguration();
        LanguageDetector langDetector = loadTikaLanguageDetector();
        MetadataExtractor extractor = new MetadataExtractor(
                new DefaultFileTypeFilter(config),
                new TikaMetadataProvider(),
                new DefaultCkanResourceFormat(langDetector, config, PACKAGE_ID),
                config);

        if (Files.isDirectory(src, new LinkOption[0])) {
            try (Stream<Path> stream = Files.list(src)) {
                stream.filter(p -> Files.isRegularFile(p, new LinkOption[0]))
                        .forEach(p -> {
                            System.out.println("Processing " + p.getFileName());
                            ProcessingReport rpt = extractor.processSource(p.toString());
                            String safe = p.getFileName().toString().replaceAll("[^a-zA-Z0-9._-]", "_");
                            String outName = "report-" + safe + ".json";
                            writeJson(rpt, outName);
                            if (!rpt.getErrors().isEmpty()) {
                                System.err.println("Voltooid met fouten voor " + p.getFileName() + ". Zie " + outName);
                            } else {
                                System.out.println("Klaar! Zie " + outName);
                            }
                        });
            } catch (IOException e) {
                System.err.println("FOUT: Kan map niet lezen: " + e.getMessage());
                System.exit(1);
            }
        } else {
            ProcessingReport report = extractor.processSource(src.toString());
            writeJson(report, "report.json");
            if (!report.getErrors().isEmpty()) {
                System.err.println("Voltooid met fouten. Zie report.json");
                System.exit(2);
            } else {
                System.out.println("Klaar! Zie report.json");
            }
        }

    }

    private static LanguageDetector loadTikaLanguageDetector() {
        try {
            LanguageDetector ld = OptimaizeLangDetector.getDefaultLanguageDetector();
            ld.loadModels();
            return ld;
        } catch (Exception e) {
            System.err.println("Waarschuwing: taaldetectie niet geladen: " + e.getMessage());
            return null;
        }
    }

    private static void writeJson(ProcessingReport rpt, String fileName) {
        Map<String, Object> root = new LinkedHashMap();
        root.put("resources", rpt.getResults().stream().map(CkanResource::getData).collect(Collectors.toList()));

        try {
            ObjectMapper mapper = (new ObjectMapper()).enable(SerializationFeature.INDENT_OUTPUT);
            mapper.writeValue(Paths.get(fileName).toFile(), root);
        } catch (IOException e) {
            System.err.println("Fout bij schrijven JSON: " + e.getMessage());
        }

    }
}

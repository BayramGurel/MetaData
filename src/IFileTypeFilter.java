/**
 * Functional interface deciding if a file should be processed.
 */
@FunctionalInterface
interface IFileTypeFilter {
    boolean isFileTypeRelevant(String entryName);
}

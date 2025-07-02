@FunctionalInterface
interface IFileTypeFilter {
    /**
     * Determines whether the given entry should be processed based on its
     * filename or extension.
     *
     * @param entryName name of the entry (may include path information)
     * @return {@code true} if the entry is relevant for extraction
     */
    boolean isFileTypeRelevant(String entryName);
}

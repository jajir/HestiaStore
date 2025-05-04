package com.coroptis.index;

import java.util.Objects;

/**
 * Logging context for index operations. Basically it shoudl be in all project
 * clases. Because it's used in all log calls.
 * 
 */
public class LoggingContext {
    private final String indexName;

    public LoggingContext(final String indexName) {
        this.indexName = Objects.requireNonNull(indexName,
                "Index name is required");
    }

    public String getIndexName() {
        return indexName;
    }

    public String formatPrefix() {
        return String.format("index '%s': ", indexName);
    }

    @Override
    public String toString() {
        return formatPrefix();
    }
}

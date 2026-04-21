package org.hestiastore.indextools;

enum ExportFormat {
    BUNDLE,
    JSONL;

    static ExportFormat parse(final String value) {
        return ExportFormat.valueOf(value.trim().toUpperCase());
    }
}

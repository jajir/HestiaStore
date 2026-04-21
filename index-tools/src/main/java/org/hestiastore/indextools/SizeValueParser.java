package org.hestiastore.indextools;

final class SizeValueParser {

    private SizeValueParser() {
    }

    static long parseBytes(final String value) {
        final String normalized = value.trim().toUpperCase();
        if (normalized.endsWith("GIB")) {
            return multiply(parseWholeNumber(normalized, "GIB"),
                    1024L * 1024L * 1024L);
        }
        if (normalized.endsWith("MIB")) {
            return multiply(parseWholeNumber(normalized, "MIB"),
                    1024L * 1024L);
        }
        if (normalized.endsWith("KIB")) {
            return multiply(parseWholeNumber(normalized, "KIB"), 1024L);
        }
        if (normalized.endsWith("GB")) {
            return multiply(parseWholeNumber(normalized, "GB"),
                    1000L * 1000L * 1000L);
        }
        if (normalized.endsWith("MB")) {
            return multiply(parseWholeNumber(normalized, "MB"), 1000L * 1000L);
        }
        if (normalized.endsWith("KB")) {
            return multiply(parseWholeNumber(normalized, "KB"), 1000L);
        }
        if (normalized.endsWith("B")) {
            return parseWholeNumber(normalized, "B");
        }
        return Long.parseLong(normalized);
    }

    private static long parseWholeNumber(final String value,
            final String suffix) {
        return Long.parseLong(
                value.substring(0, value.length() - suffix.length()).trim());
    }

    private static long multiply(final long value, final long factor) {
        return Math.multiplyExact(value, factor);
    }
}

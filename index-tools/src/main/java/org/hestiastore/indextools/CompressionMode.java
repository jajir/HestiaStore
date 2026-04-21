package org.hestiastore.indextools;

enum CompressionMode {
    NONE,
    GZIP;

    static CompressionMode parse(final String value) {
        return CompressionMode.valueOf(value.trim().toUpperCase());
    }
}

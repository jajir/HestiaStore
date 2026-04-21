package org.hestiastore.indextools;

final class ImportVerificationSummary {

    private final long recordCount;
    private final String sourceFingerprint;
    private final String targetFingerprint;

    ImportVerificationSummary(final long recordCount,
            final String sourceFingerprint, final String targetFingerprint) {
        this.recordCount = recordCount;
        this.sourceFingerprint = sourceFingerprint;
        this.targetFingerprint = targetFingerprint;
    }

    long getRecordCount() {
        return recordCount;
    }

    String getSourceFingerprint() {
        return sourceFingerprint;
    }

    String getTargetFingerprint() {
        return targetFingerprint;
    }
}

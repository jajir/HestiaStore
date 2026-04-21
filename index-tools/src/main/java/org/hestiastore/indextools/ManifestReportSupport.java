package org.hestiastore.indextools;

import java.nio.file.Path;
import java.util.LinkedHashMap;

final class ManifestReportSupport {

    private ManifestReportSupport() {
    }

    static LinkedHashMap<String, Object> summary(final Path inputDirectory,
            final ExportBundleManifest manifest, final String verificationMode) {
        final LinkedHashMap<String, Object> summary = new LinkedHashMap<>();
        summary.put("inputDirectory", inputDirectory.toString());
        summary.put("verificationMode", verificationMode);
        summary.put("format", manifest.getFormat().name().toLowerCase());
        summary.put("compression", manifest.getCompression().name().toLowerCase());
        summary.put("createdAt", manifest.getCreatedAt());
        summary.put("sourceIndexName",
                manifest.getSourceConfiguration().getIndexName());
        summary.put("sourceIndexPath", manifest.getSourceIndexPath());
        summary.put("recordCount", manifest.getRecordCount());
        summary.put("configFile", manifest.getConfigFileName());
        summary.put("fromKey", manifest.getFromKeyText());
        summary.put("toKey", manifest.getToKeyText());
        summary.put("limit", manifest.getLimit());
        if (manifest.getFormat() == ExportFormat.BUNDLE) {
            summary.put("parts", manifest.getParts());
        } else {
            summary.put("dataFile", manifest.getDataFileName());
        }
        return summary;
    }
}

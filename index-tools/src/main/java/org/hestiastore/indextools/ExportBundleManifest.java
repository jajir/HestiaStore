package org.hestiastore.indextools;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

class ExportBundleManifest {

    private int formatVersion;
    private String toolVersion;
    private Instant createdAt;
    private String sourceIndexPath;
    private ExportFormat format;
    private CompressionMode compression;
    private long maxPartSizeBytes;
    private long recordCount;
    private String dataFileName;
    private String configFileName;
    private String fromKeyText;
    private String toKeyText;
    private Long limit;
    private List<ExportPartManifest> parts = new ArrayList<>();
    private IndexConfigurationManifest sourceConfiguration;

    public int getFormatVersion() {
        return formatVersion;
    }

    public void setFormatVersion(final int formatVersion) {
        this.formatVersion = formatVersion;
    }

    public String getToolVersion() {
        return toolVersion;
    }

    public void setToolVersion(final String toolVersion) {
        this.toolVersion = toolVersion;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(final Instant createdAt) {
        this.createdAt = createdAt;
    }

    public String getSourceIndexPath() {
        return sourceIndexPath;
    }

    public void setSourceIndexPath(final String sourceIndexPath) {
        this.sourceIndexPath = sourceIndexPath;
    }

    public ExportFormat getFormat() {
        return format;
    }

    public void setFormat(final ExportFormat format) {
        this.format = format;
    }

    public CompressionMode getCompression() {
        return compression;
    }

    public void setCompression(final CompressionMode compression) {
        this.compression = compression;
    }

    public long getMaxPartSizeBytes() {
        return maxPartSizeBytes;
    }

    public void setMaxPartSizeBytes(final long maxPartSizeBytes) {
        this.maxPartSizeBytes = maxPartSizeBytes;
    }

    public long getRecordCount() {
        return recordCount;
    }

    public void setRecordCount(final long recordCount) {
        this.recordCount = recordCount;
    }

    public String getDataFileName() {
        return dataFileName;
    }

    public void setDataFileName(final String dataFileName) {
        this.dataFileName = dataFileName;
    }

    public String getConfigFileName() {
        return configFileName;
    }

    public void setConfigFileName(final String configFileName) {
        this.configFileName = configFileName;
    }

    public String getFromKeyText() {
        return fromKeyText;
    }

    public void setFromKeyText(final String fromKeyText) {
        this.fromKeyText = fromKeyText;
    }

    public String getToKeyText() {
        return toKeyText;
    }

    public void setToKeyText(final String toKeyText) {
        this.toKeyText = toKeyText;
    }

    public Long getLimit() {
        return limit;
    }

    public void setLimit(final Long limit) {
        this.limit = limit;
    }

    public List<ExportPartManifest> getParts() {
        return parts;
    }

    public void setParts(final List<ExportPartManifest> parts) {
        this.parts = parts == null ? new ArrayList<>() : new ArrayList<>(parts);
    }

    public IndexConfigurationManifest getSourceConfiguration() {
        return sourceConfiguration;
    }

    public void setSourceConfiguration(
            final IndexConfigurationManifest sourceConfiguration) {
        this.sourceConfiguration = sourceConfiguration;
    }
}

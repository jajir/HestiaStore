package org.hestiastore.indextools;

class ExportPartManifest {

    private String fileName;
    private long recordCount;
    private long encodedBytes;
    private long fileSizeBytes;

    public String getFileName() {
        return fileName;
    }

    public void setFileName(final String fileName) {
        this.fileName = fileName;
    }

    public long getRecordCount() {
        return recordCount;
    }

    public void setRecordCount(final long recordCount) {
        this.recordCount = recordCount;
    }

    public long getEncodedBytes() {
        return encodedBytes;
    }

    public void setEncodedBytes(final long encodedBytes) {
        this.encodedBytes = encodedBytes;
    }

    public long getFileSizeBytes() {
        return fileSizeBytes;
    }

    public void setFileSizeBytes(final long fileSizeBytes) {
        this.fileSizeBytes = fileSizeBytes;
    }
}

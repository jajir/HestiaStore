package org.hestiastore.indextools;

class WalManifest {

    private boolean enabled;
    private String durabilityMode;
    private long segmentSizeBytes;
    private int groupSyncDelayMillis;
    private int groupSyncMaxBatchBytes;
    private long maxBytesBeforeForcedCheckpoint;
    private String corruptionPolicy;
    private boolean epochSupport;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(final boolean enabled) {
        this.enabled = enabled;
    }

    public String getDurabilityMode() {
        return durabilityMode;
    }

    public void setDurabilityMode(final String durabilityMode) {
        this.durabilityMode = durabilityMode;
    }

    public long getSegmentSizeBytes() {
        return segmentSizeBytes;
    }

    public void setSegmentSizeBytes(final long segmentSizeBytes) {
        this.segmentSizeBytes = segmentSizeBytes;
    }

    public int getGroupSyncDelayMillis() {
        return groupSyncDelayMillis;
    }

    public void setGroupSyncDelayMillis(final int groupSyncDelayMillis) {
        this.groupSyncDelayMillis = groupSyncDelayMillis;
    }

    public int getGroupSyncMaxBatchBytes() {
        return groupSyncMaxBatchBytes;
    }

    public void setGroupSyncMaxBatchBytes(final int groupSyncMaxBatchBytes) {
        this.groupSyncMaxBatchBytes = groupSyncMaxBatchBytes;
    }

    public long getMaxBytesBeforeForcedCheckpoint() {
        return maxBytesBeforeForcedCheckpoint;
    }

    public void setMaxBytesBeforeForcedCheckpoint(
            final long maxBytesBeforeForcedCheckpoint) {
        this.maxBytesBeforeForcedCheckpoint = maxBytesBeforeForcedCheckpoint;
    }

    public String getCorruptionPolicy() {
        return corruptionPolicy;
    }

    public void setCorruptionPolicy(final String corruptionPolicy) {
        this.corruptionPolicy = corruptionPolicy;
    }

    public boolean isEpochSupport() {
        return epochSupport;
    }

    public void setEpochSupport(final boolean epochSupport) {
        this.epochSupport = epochSupport;
    }
}

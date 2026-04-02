package org.hestiastore.benchmark.diskio;

public final class DiskIoDataSupport {

    private static final int KEY_WIDTH = 10;

    private DiskIoDataSupport() {
    }

    public static String buildSequentialKey(final int value) {
        final String raw = String.valueOf(value);
        if (raw.length() >= KEY_WIDTH) {
            return raw;
        }
        final StringBuilder padded = new StringBuilder(KEY_WIDTH);
        for (int index = raw.length(); index < KEY_WIDTH; index++) {
            padded.append('0');
        }
        return padded.append(raw).toString();
    }

    public static Long buildLongValue(final int value) {
        return Long.valueOf((value * 1_103_515_245L) ^ 0x5DEECE66DL);
    }
}

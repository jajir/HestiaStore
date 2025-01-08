package com.coroptis.index;

public final class FileNameUtil {

    private FileNameUtil() {
        // prevent class instantiation
    }

    public static String getFileName(final String prefix, final int id,
            int length, final String suffix) {
        return prefix + getPaddedId(id, length) + suffix;
    }

    public static String getPaddedId(final int id, int length) {
        final StringBuilder buff = new StringBuilder(String.valueOf(id));
        if (id < 0) {
            throw new IllegalArgumentException(
                    String.format("Id '%s' is negative.", id));
        }
        if (length < 0) {
            throw new IllegalArgumentException(
                    String.format("Length '%s' is negative.", length));
        }
        if (buff.length() > length) {
            throw new IllegalArgumentException(String.format(
                    "Id '%s' is too long to be padded to '%s' positions.", id,
                    length));
        }
        while (buff.length() < length) {
            buff.insert(0, "0");
        }
        return buff.toString();
    }

}

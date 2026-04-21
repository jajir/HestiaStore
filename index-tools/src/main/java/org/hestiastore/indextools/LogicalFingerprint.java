package org.hestiastore.indextools;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

final class LogicalFingerprint {

    private final MessageDigest digest;
    private long recordCount;

    LogicalFingerprint() {
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (final NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available.", e);
        }
    }

    void update(final DescriptorSupport.DescriptorPair descriptors,
            final Object key, final Object value) {
        final byte[] keyBytes = DescriptorSupport
                .encode(descriptors.key().getDescriptor(), key);
        final byte[] valueBytes = DescriptorSupport
                .encode(descriptors.value().getDescriptor(), value);
        updateBytes(keyBytes);
        updateBytes(valueBytes);
        recordCount++;
    }

    long getRecordCount() {
        return recordCount;
    }

    String hexDigest() {
        return ChecksumSupport.toHex(digest.digest());
    }

    private void updateBytes(final byte[] bytes) {
        digest.update(intToBytes(bytes.length));
        digest.update(bytes);
    }

    private byte[] intToBytes(final int value) {
        return new byte[] { (byte) (value >>> 24), (byte) (value >>> 16),
                (byte) (value >>> 8), (byte) value };
    }
}

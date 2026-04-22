package org.hestiastore.index.segmentindex.wal;

import java.util.Arrays;
import java.util.Objects;
import java.util.zip.CRC32;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.EncodedBytes;
import org.hestiastore.index.datatype.TypeDecoder;
import org.hestiastore.index.datatype.TypeEncoder;

final class WalRecordCodec<K, V> {

    static final int MIN_RECORD_BODY_SIZE = 4 + 8 + 1 + 4 + 4;
    static final int MAX_RECORD_BODY_SIZE = 32 * 1024 * 1024;

    private final TypeEncoder<K> keyEncoder;
    private final TypeDecoder<K> keyDecoder;
    private final TypeEncoder<V> valueEncoder;
    private final TypeDecoder<V> valueDecoder;

    WalRecordCodec(final TypeEncoder<K> keyEncoder,
            final TypeDecoder<K> keyDecoder,
            final TypeEncoder<V> valueEncoder,
            final TypeDecoder<V> valueDecoder) {
        this.keyEncoder = keyEncoder;
        this.keyDecoder = keyDecoder;
        this.valueEncoder = valueEncoder;
        this.valueDecoder = valueDecoder;
    }

    byte[] encodeRecord(final WalRuntime.Operation operation, final long lsn,
            final K key, final V value) {
        final byte[] keyBytes = encodeKey(key);
        final byte[] valueBytes = operation == WalRuntime.Operation.PUT
                ? encodeValue(value)
                : new byte[0];
        final int bodyLen = MIN_RECORD_BODY_SIZE + keyBytes.length
                + valueBytes.length;
        if (bodyLen > MAX_RECORD_BODY_SIZE) {
            throw new IllegalArgumentException(String.format(
                    "WAL record body is too large: %s", bodyLen));
        }
        final byte[] body = new byte[bodyLen];
        int offset = 0;
        offset += 4;
        putLong(body, offset, lsn);
        offset += 8;
        body[offset++] = operation.code();
        putInt(body, offset, keyBytes.length);
        offset += 4;
        putInt(body, offset, valueBytes.length);
        offset += 4;
        System.arraycopy(keyBytes, 0, body, offset, keyBytes.length);
        offset += keyBytes.length;
        System.arraycopy(valueBytes, 0, body, offset, valueBytes.length);
        putInt(body, 0, computeCrc32(body, 4, body.length - 4));
        final byte[] encoded = new byte[4 + body.length];
        putInt(encoded, 0, body.length);
        System.arraycopy(body, 0, encoded, 4, body.length);
        return encoded;
    }

    WalDecodedRecord<K, V> decodeBody(final byte[] body,
            final long previousLsn) {
        final int storedCrc = readInt(body, 0);
        final int computedCrc = computeCrc32(body, 4, body.length - 4);
        if (storedCrc != computedCrc) {
            throw new IndexException("Invalid WAL record CRC.");
        }
        int position = 4;
        final long lsn = readLong(body, position);
        position += 8;
        final WalRuntime.Operation operation = WalRuntime.Operation
                .fromCode(body[position++]);
        if (operation == null) {
            throw new IndexException("Invalid WAL operation code.");
        }
        final int keyLen = readInt(body, position);
        position += 4;
        final int valueLen = readInt(body, position);
        position += 4;
        if (keyLen <= 0 || valueLen < 0
                || position + keyLen + valueLen != body.length) {
            throw new IndexException("Invalid WAL record body lengths.");
        }
        if (lsn <= 0L || lsn <= previousLsn) {
            throw new IndexException("Invalid WAL LSN ordering.");
        }
        final byte[] keyBytes = new byte[keyLen];
        System.arraycopy(body, position, keyBytes, 0, keyLen);
        position += keyLen;
        final byte[] valueBytes = new byte[valueLen];
        if (valueLen > 0) {
            System.arraycopy(body, position, valueBytes, 0, valueLen);
        }
        if (operation == WalRuntime.Operation.DELETE && valueLen != 0) {
            throw new IndexException("Invalid WAL delete payload.");
        }
        final K key = keyDecoder.decode(keyBytes);
        final V value = operation == WalRuntime.Operation.PUT
                ? valueDecoder.decode(valueBytes)
                : null;
        return new WalDecodedRecord<>(lsn, operation, key, value);
    }

    boolean isBodyLengthValid(final int bodyLength) {
        return bodyLength >= MIN_RECORD_BODY_SIZE
                && bodyLength <= MAX_RECORD_BODY_SIZE;
    }

    private byte[] encodeKey(final K key) {
        final EncodedBytes encoded = keyEncoder.encode(Objects.requireNonNull(key,
                "key"), new byte[0]);
        return toExactArray(encoded, "key");
    }

    private byte[] encodeValue(final V value) {
        final EncodedBytes encoded = valueEncoder.encode(
                Objects.requireNonNull(value, "value"), new byte[0]);
        return toExactArray(encoded, "value");
    }

    private byte[] toExactArray(final EncodedBytes encoded,
            final String fieldName) {
        final EncodedBytes validated = Vldtn.requireNonNull(encoded, "encoded");
        final int length = Vldtn.requireGreaterThanOrEqualToZero(
                validated.getLength(), fieldName + "Length");
        final byte[] bytes = validated.getBytes();
        if (bytes.length == length) {
            return bytes;
        }
        return Arrays.copyOf(bytes, length);
    }

    static int computeCrc32(final byte[] data, final int offset,
            final int length) {
        final CRC32 crc32 = new CRC32();
        crc32.update(data, offset, length);
        return (int) crc32.getValue();
    }

    static void putInt(final byte[] bytes, final int offset, final int value) {
        bytes[offset] = (byte) (value >>> 24);
        bytes[offset + 1] = (byte) (value >>> 16);
        bytes[offset + 2] = (byte) (value >>> 8);
        bytes[offset + 3] = (byte) value;
    }

    static int readInt(final byte[] bytes, final int offset) {
        return ((bytes[offset] & 0xFF) << 24)
                | ((bytes[offset + 1] & 0xFF) << 16)
                | ((bytes[offset + 2] & 0xFF) << 8)
                | (bytes[offset + 3] & 0xFF);
    }

    static void putLong(final byte[] bytes, final int offset, final long value) {
        bytes[offset] = (byte) (value >>> 56);
        bytes[offset + 1] = (byte) (value >>> 48);
        bytes[offset + 2] = (byte) (value >>> 40);
        bytes[offset + 3] = (byte) (value >>> 32);
        bytes[offset + 4] = (byte) (value >>> 24);
        bytes[offset + 5] = (byte) (value >>> 16);
        bytes[offset + 6] = (byte) (value >>> 8);
        bytes[offset + 7] = (byte) value;
    }

    static long readLong(final byte[] bytes, final int offset) {
        return ((long) (bytes[offset] & 0xFF) << 56)
                | ((long) (bytes[offset + 1] & 0xFF) << 48)
                | ((long) (bytes[offset + 2] & 0xFF) << 40)
                | ((long) (bytes[offset + 3] & 0xFF) << 32)
                | ((long) (bytes[offset + 4] & 0xFF) << 24)
                | ((long) (bytes[offset + 5] & 0xFF) << 16)
                | ((long) (bytes[offset + 6] & 0xFF) << 8)
                | ((long) bytes[offset + 7] & 0xFF);
    }
}

package org.hestiastore.index.segmentindex.wal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.datatype.TypeDescriptorString;
import org.junit.jupiter.api.Test;

class WalRecordCodecTest {

    private static final TypeDescriptorString STRING_DESCRIPTOR = new TypeDescriptorString();

    @Test
    void encodeAndDecodePutRoundTrips() {
        final WalRecordCodec<String, String> codec = new WalRecordCodec<>(
                STRING_DESCRIPTOR.getTypeEncoder(),
                STRING_DESCRIPTOR.getTypeDecoder(),
                STRING_DESCRIPTOR.getTypeEncoder(),
                STRING_DESCRIPTOR.getTypeDecoder());

        final byte[] bytes = codec.encodeRecord(WalRuntime.Operation.PUT, 7L,
                "key", "value");
        final int bodyLength = WalRuntime.readInt(bytes, 0);
        final byte[] body = new byte[bodyLength];
        System.arraycopy(bytes, 4, body, 0, body.length);

        final WalDecodedRecord<String, String> decoded = codec.decodeBody(body,
                0L);

        assertEquals(7L, decoded.lsn());
        assertEquals(WalRuntime.Operation.PUT, decoded.operation());
        assertEquals("key", decoded.key());
        assertEquals("value", decoded.value());
    }

    @Test
    void decodeRejectsRegressingLsn() {
        final WalRecordCodec<String, String> codec = new WalRecordCodec<>(
                STRING_DESCRIPTOR.getTypeEncoder(),
                STRING_DESCRIPTOR.getTypeDecoder(),
                STRING_DESCRIPTOR.getTypeEncoder(),
                STRING_DESCRIPTOR.getTypeDecoder());

        final byte[] bytes = codec.encodeRecord(WalRuntime.Operation.DELETE, 3L,
                "key", null);
        final int bodyLength = WalRuntime.readInt(bytes, 0);
        final byte[] body = new byte[bodyLength];
        System.arraycopy(bytes, 4, body, 0, body.length);

        assertThrows(IndexException.class, () -> codec.decodeBody(body, 3L));
    }
}

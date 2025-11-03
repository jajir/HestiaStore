package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.zip.CRC32;
import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.ByteSequenceView;
import org.hestiastore.index.bytes.ByteSequences;
import org.hestiastore.index.bytes.MutableBytes;
import org.junit.jupiter.api.Test;

class ChunkPayloadTest {

    @Test
    void of_returnsSameInstanceWhenBytesAlreadyProvided() {
        final ByteSequenceView data = ByteSequenceView
                .of(new byte[] { 1, 2, 3 });

        final ChunkPayload payload = ChunkPayload.of(data);

        assertSame(data, payload.getBytes());
    }

    @Test
    void of_wrapsByteSequenceWithoutCopying() {
        final MutableBytes buffer = MutableBytes.allocate(3);
        buffer.setByte(0, (byte) 1);
        buffer.setByte(1, (byte) 2);
        buffer.setByte(2, (byte) 3);
        final ByteSequence view = buffer.toByteSequence();

        final ChunkPayload payload = ChunkPayload.of(view);

        buffer.setByte(0, (byte) 9);
        assertEquals(9, payload.getBytes().getByte(0));
    }

    @Test
    void of_nullBytesThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> ChunkPayload.of(null));
    }

    @Test
    void equals_comparesContent() {
        final ChunkPayload first = ChunkPayload
                .of(ByteSequences.wrap(new byte[] { 1, 2, 3 }));
        final MutableBytes secondBuffer = MutableBytes
                .copyOf(ByteSequences.wrap(new byte[] { 1, 2, 3 }));
        final ChunkPayload second = ChunkPayload
                .of(secondBuffer.toByteSequence());

        assertEquals(first, second);
        assertEquals(first.hashCode(), second.hashCode());

        secondBuffer.setByte(2, (byte) 9);
        final ChunkPayload third = ChunkPayload
                .of(secondBuffer.toByteSequence());

        assertNotEquals(first, third);
    }

    @Test
    void calculateCrc_matchesReferenceImplementation() {
        final byte[][] samples = { {}, { 0 }, { 1, 2, 3, 4 },
                "chunk payload".getBytes(),
                { (byte) 0xFF, (byte) 0xEE, (byte) 0xDD, (byte) 0xCC } };

        for (byte[] sample : samples) {
            final CRC32 reference = new CRC32();
            reference.update(sample, 0, sample.length);

            final ChunkPayload payload = ChunkPayload
                    .of(ByteSequences.wrap(sample));

            assertEquals(reference.getValue(), payload.calculateCrc());
        }
    }
}

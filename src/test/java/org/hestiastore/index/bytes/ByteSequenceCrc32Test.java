package org.hestiastore.index.bytes;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.zip.CRC32;

import org.junit.jupiter.api.Test;

class ByteSequenceCrc32Test {

    @Test
    void update_matchesPureJavaImplementation() {
        final byte[][] samples = { {}, { 0 }, { (byte) 0xFF },
                { 1, 2, 3, 4, 5 }, "hello world".getBytes(),
                new byte[] { (byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF,
                        (byte) 0x42 } };

        for (byte[] sample : samples) {
            final CRC32 reference = new CRC32();
            reference.update(sample, 0, sample.length);

            final ByteSequenceCrc32 underTest = new ByteSequenceCrc32();
            underTest.update(Bytes.of(sample));

            assertEquals(reference.getValue(), underTest.getValue());
        }
    }

    @Test
    void update_multipleCallsAccumulate() {
        final CRC32 reference = new CRC32();
        reference.update(new byte[] { 1, 2, 3 }, 0, 3);
        reference.update(new byte[] { 4, 5 }, 0, 2);

        final ByteSequenceCrc32 underTest = new ByteSequenceCrc32();
        underTest.update(Bytes.of(new byte[] { 1, 2, 3 }));
        underTest.update(Bytes.of(new byte[] { 4, 5 }));

        assertEquals(reference.getValue(), underTest.getValue());
    }
}

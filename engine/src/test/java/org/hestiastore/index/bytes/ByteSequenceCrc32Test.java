package org.hestiastore.index.bytes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.zip.CRC32;

import org.junit.jupiter.api.Test;

class ByteSequenceCrc32Test {

    @Test
    void test_crc_matches_jdk_crc32_for_byte_array_update() {
        final byte[] data = new byte[] { 1, 2, 3, 4, 5 };
        final ByteSequenceCrc32 crc = new ByteSequenceCrc32();
        final CRC32 expected = new CRC32();

        crc.update(data, 1, 3);
        expected.update(data, 1, 3);

        assertEquals(expected.getValue(), crc.getValue());
    }

    @Test
    void test_crc_matches_jdk_crc32_for_byte_sequence_update() {
        final byte[] data = new byte[] { 9, 8, 7, 6 };
        final ByteSequence sequence = ByteSequences.wrap(data);
        final ByteSequenceCrc32 crc = new ByteSequenceCrc32();
        final CRC32 expected = new CRC32();

        crc.update(sequence);
        expected.update(data, 0, data.length);

        assertEquals(expected.getValue(), crc.getValue());
    }

    @Test
    void test_crc_matches_jdk_crc32_for_byte_sequence_slice_update() {
        final byte[] data = new byte[] { 11, 12, 13, 14, 15, 16 };
        final ByteSequence sequence = ByteSequences.viewOf(data, 1, 5);
        final ByteSequenceCrc32 crc = new ByteSequenceCrc32();
        final CRC32 expected = new CRC32();

        crc.update(sequence);
        expected.update(data, 1, 4);

        assertEquals(expected.getValue(), crc.getValue());
    }

    @Test
    void test_crc_matches_jdk_crc32_for_mutable_bytes_update() {
        final byte[] data = new byte[] { 4, 3, 2, 1, 0 };
        final MutableBytes sequence = MutableBytes.wrap(data);
        final ByteSequenceCrc32 crc = new ByteSequenceCrc32();
        final CRC32 expected = new CRC32();

        crc.update(sequence);
        expected.update(data, 0, data.length);

        assertEquals(expected.getValue(), crc.getValue());
    }

    @Test
    void test_update_validates_input() {
        final ByteSequenceCrc32 crc = new ByteSequenceCrc32();

        assertThrows(IllegalArgumentException.class, () -> crc.update(null, 0, 0));
        assertThrows(IllegalArgumentException.class,
                () -> crc.update(new byte[2], -1, 1));
        assertThrows(IllegalArgumentException.class,
                () -> crc.update(new byte[2], 1, 2));
        assertThrows(IllegalArgumentException.class, () -> crc.update((ByteSequence) null));
    }

    @Test
    void test_reset_restarts_crc_state() {
        final ByteSequenceCrc32 crc = new ByteSequenceCrc32();
        crc.update(new byte[] { 1, 2, 3 }, 0, 3);
        final long beforeReset = crc.getValue();

        crc.reset();
        crc.update(new byte[] { 1, 2, 3 }, 0, 3);

        assertEquals(beforeReset, crc.getValue());
    }
}

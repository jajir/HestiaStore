package org.hestiastore.index.chunkentryfile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.directory.MemFileReader;
import org.junit.jupiter.api.Test;

class LongKeyPageReaderTest {

    @Test
    void encodedReadsRetainRanksAndShareFramingStateWithLogicalReads() {
        final var codec = KeyPageCodecs.longFixedWeightDeltaVarint(4, 2,
                new long[0], 0);
        final var decoder = new LongKeyPageReader(codec);
        try (var input = new MemFileReader(
                new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 1, 4 })) {
            assertEquals(0, decoder.readEncodedLong(input));
            assertEquals(5, decoder.readLong(input));
            assertEquals(5, decoder.readEncodedLong(input));
            assertNull(decoder.read(input));
        }
    }

    @Test
    void encodedReadsStillRejectInvalidRanksAndMalformedDeltas() {
        final var codec = KeyPageCodecs.longFixedWeightDeltaVarint(4, 2,
                new long[0], 0);
        for (final byte[] invalid : new byte[][] { { 0, 0, 0, 0, 0, 0, 0, 6 },
                { (byte) 128, 0, 0, 0, 0, 0, 0, 0 }, { 0, 0, 0 } }) {
            final var decoder = new LongKeyPageReader(codec);
            try (var input = new MemFileReader(invalid)) {
                assertThrows(IndexException.class,
                        () -> decoder.readEncodedLong(input));
            }
        }
        for (final byte[] tail : new byte[][] { { 0 }, { 1 }, { (byte) 128, 0 },
                { (byte) 128 } }) {
            final byte[] bytes = new byte[8 + tail.length];
            bytes[7] = 5;
            System.arraycopy(tail, 0, bytes, 8, tail.length);
            final var decoder = new LongKeyPageReader(codec);
            try (var input = new MemFileReader(bytes)) {
                assertEquals(5, decoder.readEncodedLong(input));
                assertThrows(IndexException.class,
                        () -> decoder.readEncodedLong(input));
            }
        }
    }

    @Test
    void fixedWeightReturnsLogicalKeysAndRejectsInvalidAbsoluteAndDeltaRanks() {
        final var codec = KeyPageCodecs.longFixedWeightDeltaVarint(4, 2,
                new long[0], 0);
        final var writer = new LongKeyPageWriter(codec);
        final var output = new InMemoryFileWriter();
        final long[] keys = { 3, 5, 6, 9, 10, 12 };
        for (final long key : keys) {
            writer.write(output, key);
        }
        final var decoder = new LongKeyPageReader(codec);
        try (var input = new MemFileReader(output.closeSequence())) {
            for (final long key : keys) {
                assertEquals(key, decoder.readLong(input));
            }
            assertNull(decoder.read(input));
        }
        for (final byte[] invalid : new byte[][] { { 0, 0, 0, 0, 0, 0, 0, 6 },
                { (byte) 128, 0, 0, 0, 0, 0, 0, 0 } }) {
            final var malformed = new LongKeyPageReader(codec);
            try (var input = new MemFileReader(invalid)) {
                assertThrows(IndexException.class,
                        () -> malformed.readLong(input));
            }
        }
        final var malformed = new LongKeyPageReader(codec);
        try (var input = new MemFileReader(
                new byte[] { 0, 0, 0, 0, 0, 0, 0, 5, 1 })) {
            assertEquals(12, malformed.readLong(input));
            assertThrows(IndexException.class, () -> malformed.readLong(input));
        }
    }

    @Test
    void bothCodecsRoundTripSignedExtremesAndRandomGaps() {
        final long[] random = new Random(77).longs(2000).toArray();
        random[0] = Long.MIN_VALUE;
        random[1] = Long.MAX_VALUE;
        Arrays.sort(random);
        for (var codec : List.of(KeyPageCodecs.<Long>prefix(),
                KeyPageCodecs.longDeltaVarint())) {
            final var writer = new LongKeyPageWriter(codec);
            final var bytes = new InMemoryFileWriter();
            for (long key : random)
                writer.write(bytes, key);
            final var decoder = new LongKeyPageReader(codec);
            try (var reader = new MemFileReader(bytes.closeSequence())) {
                for (long key : random)
                    assertEquals(key, decoder.readLong(reader));
                assertNull(decoder.read(reader));
                assertThrows(IndexException.class,
                        () -> decoder.readLong(reader));
            }
        }
    }

    @Test
    void decodesTenByteDeltaAcrossEntireSignedRange() {
        final var out = new InMemoryFileWriter();
        final var writer = new LongKeyPageWriter(
                KeyPageCodecs.longDeltaVarint());
        writer.write(out, Long.MIN_VALUE);
        writer.write(out, Long.MAX_VALUE);
        final var decoder = new LongKeyPageReader(
                KeyPageCodecs.longDeltaVarint());
        try (var in = new MemFileReader(out.closeSequence())) {
            assertEquals(Long.MIN_VALUE, decoder.readLong(in));
            assertEquals(Long.MAX_VALUE, decoder.readLong(in));
        }
    }

    @Test
    void rejectsZeroOverlongTruncatedOverflowAndWrappedDeltas() {
        final byte[][] invalid = { { 0 }, { (byte) 128, 0 }, { (byte) 128 },
                { (byte) 255, (byte) 255, (byte) 255, (byte) 255, (byte) 255,
                        (byte) 255, (byte) 255, (byte) 255, (byte) 255, 2 },
                { (byte) 255, (byte) 255, (byte) 255, (byte) 255, (byte) 255,
                        (byte) 255, (byte) 255, (byte) 255, (byte) 255, 1 } };
        for (byte[] tail : invalid) {
            byte[] page = new byte[8 + tail.length];
            page[7] = 1;
            System.arraycopy(tail, 0, page, 8, tail.length);
            final var decoder = new LongKeyPageReader(
                    KeyPageCodecs.longDeltaVarint());
            try (var in = new MemFileReader(page)) {
                assertEquals(1L, decoder.readLong(in));
                assertThrows(IndexException.class, () -> decoder.readLong(in));
            }
        }
    }

    @Test
    void rejectsTruncatedFirstKeyAndInvalidPrefix() {
        try (var in = new MemFileReader(new byte[] { 0, 0, 1 })) {
            final var decoder = new LongKeyPageReader(
                    KeyPageCodecs.longDeltaVarint());
            assertThrows(IndexException.class, () -> decoder.readLong(in));
        }
        for (byte[] data : new byte[][] { { 1, 7, 1, 2, 3, 4, 5, 6, 7 },
                { 0, 7, 0 }, { 8, 0 } }) {
            try (var in = new MemFileReader(data)) {
                final var decoder = new LongKeyPageReader(
                        KeyPageCodecs.prefix());
                assertThrows(IndexException.class, () -> decoder.readLong(in));
            }
        }
    }
}

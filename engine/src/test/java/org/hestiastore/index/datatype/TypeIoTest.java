package org.hestiastore.index.datatype;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.directory.FileReader;
import org.hestiastore.index.directory.MemFileReader;
import org.junit.jupiter.api.Test;

class TypeIoTest {

    @Test
    void readFullyOrNull_returnsFalseWhenNoDataAreAvailable() {
        final boolean wasRead = TypeIo.readFullyOrNull(
                new MemFileReader(new byte[0]), new byte[4]);

        assertFalse(wasRead);
    }

    @Test
    void readFullyOrNull_readsAllRequestedBytes() {
        final byte[] destination = new byte[3];

        final boolean wasRead = TypeIo.readFullyOrNull(
                new MemFileReader(new byte[] { 1, (byte) 0xFF, 3 }),
                destination);

        assertTrue(wasRead);
        assertArrayEquals(new byte[] { 1, (byte) 0xFF, 3 }, destination);
    }

    @Test
    void readFullyOrNull_throwsOnTruncatedData() {
        final IndexException error = assertThrows(IndexException.class,
                () -> TypeIo.readFullyOrNull(new MemFileReader(new byte[] { 1,
                        2, 3 }), new byte[4]));

        assertTrue(error.getMessage().contains("Expected '4' bytes"));
        assertTrue(error.getMessage().contains("EOF"));
    }

    @Test
    void readFullyRequired_throwsWhenNoDataAreAvailable() {
        final IndexException error = assertThrows(IndexException.class,
                () -> TypeIo.readFullyRequired(new MemFileReader(new byte[0]),
                        new byte[1]));

        assertTrue(error.getMessage().contains("before reading any data"));
    }

    @Test
    void readFullyOrNull_readsAcrossPartialRangeReads() {
        final byte[] destination = new byte[5];
        final FileReader reader = new PartialChunkFileReader(
                new byte[] { 10, 11, 12, 13, 14 }, 2, false);

        final boolean wasRead = TypeIo.readFullyOrNull(reader, destination);

        assertTrue(wasRead);
        assertArrayEquals(new byte[] { 10, 11, 12, 13, 14 }, destination);
    }

    @Test
    void readFullyOrNull_throwsWhenReaderReturnsZeroBytes() {
        final FileReader reader = new PartialChunkFileReader(
                new byte[] { 1, 2, 3 }, 2, true);
        final IndexException error = assertThrows(IndexException.class,
                () -> TypeIo.readFullyOrNull(reader, new byte[3]));

        assertTrue(error.getMessage().contains("returned 0 bytes"));
    }

    private static final class PartialChunkFileReader
            extends AbstractCloseableResource implements FileReader {

        private final byte[] data;
        private final int maxChunkSize;
        private final boolean returnZeroOnce;
        private int position = 0;
        private boolean zeroWasReturned = false;

        private PartialChunkFileReader(final byte[] data, final int maxChunkSize,
                final boolean returnZeroOnce) {
            this.data = data;
            this.maxChunkSize = maxChunkSize;
            this.returnZeroOnce = returnZeroOnce;
        }

        @Override
        public int read() {
            if (position >= data.length) {
                return -1;
            }
            return data[position++] & 0xFF;
        }

        @Override
        public int read(final byte[] bytes) {
            return read(bytes, 0, bytes.length);
        }

        @Override
        public int read(final byte[] bytes, final int offset,
                final int length) {
            if (returnZeroOnce && !zeroWasReturned) {
                zeroWasReturned = true;
                return 0;
            }
            if (position >= data.length) {
                return -1;
            }
            final int toRead = Math.min(length,
                    Math.min(maxChunkSize, data.length - position));
            System.arraycopy(data, position, bytes, offset, toRead);
            position += toRead;
            return toRead;
        }

        @Override
        public void skip(final long skipLength) {
            position = (int) Math.min(data.length, position + skipLength);
        }

        @Override
        protected void doClose() {
            // no-op
        }
    }
}

package org.hestiastore.index.directory.async;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.junit.jupiter.api.Test;

class AsyncBlockingFileAdaptersRangeTest {

    @Test
    void readerBlockingAdapter_usesDelegateRangeRead() {
        final RecordingAsyncFileReader delegate = new RecordingAsyncFileReader();
        final byte[] target = new byte[] { 0, 0, 0, 0, 0 };

        try (final AsyncFileReaderBlockingAdapter adapter =
                new AsyncFileReaderBlockingAdapter(delegate)) {
            final int read = adapter.read(target, 1, 3);

            assertEquals(3, read);
            assertSame(target, delegate.lastReadBytes);
            assertEquals(1, delegate.lastReadOffset);
            assertEquals(3, delegate.lastReadLength);
            assertArrayEquals(new byte[] { 0, 11, 12, 13, 0 }, target);
        }
    }

    @Test
    void seekableReaderBlockingAdapter_usesDelegateRangeRead() {
        final RecordingAsyncFileReaderSeekable delegate =
                new RecordingAsyncFileReaderSeekable();
        final byte[] target = new byte[] { 9, 9, 9, 9 };

        try (final AsyncFileReaderSeekableBlockingAdapter adapter =
                new AsyncFileReaderSeekableBlockingAdapter(delegate)) {
            final int read = adapter.read(target, 0, 2);

            assertEquals(2, read);
            assertSame(target, delegate.lastReadBytes);
            assertEquals(0, delegate.lastReadOffset);
            assertEquals(2, delegate.lastReadLength);
            assertArrayEquals(new byte[] { 11, 12, 9, 9 }, target);
        }
    }

    @Test
    void writerBlockingAdapter_usesDelegateRangeWrite() {
        final RecordingAsyncFileWriter delegate = new RecordingAsyncFileWriter();
        final byte[] source = new byte[] { 1, 2, 3, 4, 5 };

        try (final AsyncFileWriterBlockingAdapter adapter =
                new AsyncFileWriterBlockingAdapter(delegate)) {
            adapter.write(source, 2, 2);

            assertSame(source, delegate.lastWriteBytes);
            assertEquals(2, delegate.lastWriteOffset);
            assertEquals(2, delegate.lastWriteLength);
            assertFalse(delegate.wroteWholeArray);
        }
    }

    private static class RecordingAsyncFileReader implements AsyncFileReader {
        protected byte[] lastReadBytes;
        protected int lastReadOffset;
        protected int lastReadLength;
        protected boolean closed;

        @Override
        public CompletionStage<Integer> readAsync() {
            return CompletableFuture.completedFuture(Integer.valueOf(-1));
        }

        @Override
        public CompletionStage<Integer> readAsync(final byte[] bytes) {
            return CompletableFuture.completedFuture(Integer.valueOf(-1));
        }

        @Override
        public CompletionStage<Integer> readAsync(final byte[] bytes,
                final int offset, final int length) {
            this.lastReadBytes = bytes;
            this.lastReadOffset = offset;
            this.lastReadLength = length;
            for (int i = 0; i < length; i++) {
                bytes[offset + i] = (byte) (11 + i);
            }
            return CompletableFuture.completedFuture(Integer.valueOf(length));
        }

        @Override
        public CompletionStage<Void> skipAsync(final long position) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void close() {
            closed = true;
        }

        @Override
        public boolean wasClosed() {
            return closed;
        }
    }

    private static final class RecordingAsyncFileReaderSeekable
            extends RecordingAsyncFileReader implements AsyncFileReaderSeekable {

        @Override
        public CompletionStage<Void> seekAsync(final long position) {
            return CompletableFuture.completedFuture(null);
        }
    }

    private static final class RecordingAsyncFileWriter implements AsyncFileWriter {
        private byte[] lastWriteBytes;
        private int lastWriteOffset;
        private int lastWriteLength;
        private boolean wroteWholeArray;
        private boolean closed;

        @Override
        public CompletionStage<Void> writeAsync(final byte b) {
            wroteWholeArray = false;
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<Void> writeAsync(final byte[] bytes) {
            wroteWholeArray = true;
            lastWriteBytes = bytes;
            lastWriteOffset = 0;
            lastWriteLength = bytes.length;
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<Void> writeAsync(final byte[] bytes,
                final int offset, final int length) {
            wroteWholeArray = false;
            lastWriteBytes = bytes;
            lastWriteOffset = offset;
            lastWriteLength = length;
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void close() {
            closed = true;
        }

        @Override
        public boolean wasClosed() {
            return closed;
        }
    }
}

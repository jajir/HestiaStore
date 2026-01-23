package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.hestiastore.index.segment.SegmentTestHelper.closeAndAwait;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FileLock;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.directory.async.AsyncDirectory;
import org.hestiastore.index.directory.async.AsyncDirectoryAdapter;
import org.hestiastore.index.directory.async.AsyncFileReader;
import org.hestiastore.index.directory.async.AsyncFileReaderSeekable;
import org.hestiastore.index.directory.async.AsyncFileWriter;
import org.junit.jupiter.api.Test;

class SegmentCompactionPublishTest {

    private static final TypeDescriptorInteger KEY_DESCRIPTOR = new TypeDescriptorInteger();
    private static final TypeDescriptorShortString VALUE_DESCRIPTOR = new TypeDescriptorShortString();

    @Test
    void publish_compaction_does_not_touch_io() throws Exception {
        final GuardedAsyncDirectory guardedDirectory = new GuardedAsyncDirectory(
                AsyncDirectoryAdapter.wrap(new MemDirectory()));
        final SegmentId segmentId = SegmentId.of(1);
        final Segment<Integer, String> segment = Segment
                .<Integer, String>builder(guardedDirectory)//
                .withId(segmentId)//
                .withKeyTypeDescriptor(KEY_DESCRIPTOR)//
                .withValueTypeDescriptor(VALUE_DESCRIPTOR)//
                .withMaxNumberOfKeysInSegmentWriteCache(8)//
                .withMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance(16)//
                .withMaxNumberOfKeysInSegmentCache(16)//
                .withMaxNumberOfKeysInSegmentChunk(4)//
                .withBloomFilterProbabilityOfFalsePositive(0.01)//
                .withDiskIoBufferSize(1024)//
                .withEncodingChunkFilters(
                        List.of(new ChunkFilterDoNothing()))//
                .withDecodingChunkFilters(
                        List.of(new ChunkFilterDoNothing()))//
                .withSegmentMaintenanceAutoEnabled(false)//
                .build();
        try {
            assertEquals(SegmentResultStatus.OK,
                    segment.put(1, "one").getStatus());
            assertEquals(SegmentResultStatus.OK,
                    segment.put(2, "two").getStatus());

            final SegmentImpl<Integer, String> impl = (SegmentImpl<Integer, String>) segment;
            final SegmentCompacter<Integer, String> compacter = getField(impl,
                    "segmentCompacter");
            final SegmentCore<Integer, String> core = getField(impl, "core");
            final SegmentCompacter.CompactionPlan<Integer, String> plan = compacter
                    .prepareCompactionPlan(core);
            compacter.writeCompaction(plan);

            guardedDirectory.blockIo();
            assertDoesNotThrow(() -> compacter.publishCompaction(plan));
            guardedDirectory.allowIo();
        } finally {
            closeAndAwait(segment);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T getField(final Object target, final String fieldName)
            throws Exception {
        final Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return (T) field.get(target);
    }

    private static final class GuardedAsyncDirectory implements AsyncDirectory {

        private final AsyncDirectory delegate;
        private final AtomicBoolean ioBlocked;

        private GuardedAsyncDirectory(final AsyncDirectory delegate) {
            this(delegate, new AtomicBoolean(false));
        }

        private GuardedAsyncDirectory(final AsyncDirectory delegate,
                final AtomicBoolean ioBlocked) {
            this.delegate = delegate;
            this.ioBlocked = ioBlocked;
        }

        void blockIo() {
            ioBlocked.set(true);
        }

        void allowIo() {
            ioBlocked.set(false);
        }

        @Override
        public CompletionStage<AsyncFileReader> getFileReaderAsync(
                final String fileName) {
            ensureAllowed("getFileReaderAsync");
            return delegate.getFileReaderAsync(fileName);
        }

        @Override
        public CompletionStage<AsyncFileReader> getFileReaderAsync(
                final String fileName, final int bufferSize) {
            ensureAllowed("getFileReaderAsync(buffer)");
            return delegate.getFileReaderAsync(fileName, bufferSize);
        }

        @Override
        public CompletionStage<AsyncFileReaderSeekable> getFileReaderSeekableAsync(
                final String fileName) {
            ensureAllowed("getFileReaderSeekableAsync");
            return delegate.getFileReaderSeekableAsync(fileName);
        }

        @Override
        public CompletionStage<AsyncFileWriter> getFileWriterAsync(
                final String fileName) {
            ensureAllowed("getFileWriterAsync");
            return delegate.getFileWriterAsync(fileName);
        }

        @Override
        public CompletionStage<AsyncFileWriter> getFileWriterAsync(
                final String fileName, final Directory.Access access) {
            ensureAllowed("getFileWriterAsync(access)");
            return delegate.getFileWriterAsync(fileName, access);
        }

        @Override
        public CompletionStage<AsyncFileWriter> getFileWriterAsync(
                final String fileName, final Directory.Access access,
                final int bufferSize) {
            ensureAllowed("getFileWriterAsync(access,buffer)");
            return delegate.getFileWriterAsync(fileName, access, bufferSize);
        }

        @Override
        public CompletionStage<Boolean> isFileExistsAsync(final String fileName) {
            ensureAllowed("isFileExistsAsync");
            return delegate.isFileExistsAsync(fileName);
        }

        @Override
        public CompletionStage<Boolean> deleteFileAsync(final String fileName) {
            ensureAllowed("deleteFileAsync");
            return delegate.deleteFileAsync(fileName);
        }

        @Override
        public CompletionStage<Stream<String>> getFileNamesAsync() {
            ensureAllowed("getFileNamesAsync");
            return delegate.getFileNamesAsync();
        }

        @Override
        public CompletionStage<Void> renameFileAsync(final String currentFileName,
                final String newFileName) {
            ensureAllowed("renameFileAsync");
            return delegate.renameFileAsync(currentFileName, newFileName);
        }

        @Override
        public CompletionStage<AsyncDirectory> openSubDirectory(
                final String directoryName) {
            ensureAllowed("openSubDirectory");
            return delegate.openSubDirectory(directoryName)
                    .thenApply(dir -> new GuardedAsyncDirectory(dir, ioBlocked));
        }

        @Override
        public CompletionStage<Boolean> rmdir(final String directoryName) {
            ensureAllowed("rmdir");
            return delegate.rmdir(directoryName);
        }

        @Override
        public CompletionStage<FileLock> getLockAsync(final String fileName) {
            ensureAllowed("getLockAsync");
            return delegate.getLockAsync(fileName);
        }

        @Override
        public boolean wasClosed() {
            return delegate.wasClosed();
        }

        @Override
        public void close() {
            delegate.close();
        }

        private void ensureAllowed(final String operation) {
            if (ioBlocked.get()) {
                throw new IllegalStateException(
                        "IO blocked during publish: " + operation);
            }
        }
    }
}

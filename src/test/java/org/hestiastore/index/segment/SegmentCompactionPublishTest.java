package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.hestiastore.index.segment.SegmentTestHelper.closeAndAwait;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FileReader;
import org.hestiastore.index.directory.FileReaderSeekable;
import org.hestiastore.index.directory.FileLock;
import org.hestiastore.index.directory.FileWriter;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.Test;

class SegmentCompactionPublishTest {

    private static final TypeDescriptorInteger KEY_DESCRIPTOR = new TypeDescriptorInteger();
    private static final TypeDescriptorShortString VALUE_DESCRIPTOR = new TypeDescriptorShortString();

    @Test
    void publish_compaction_does_not_touch_io() throws Exception {
        final GuardedDirectory guardedDirectory = new GuardedDirectory(
                new MemDirectory());
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
                .withMaintenancePolicy(SegmentMaintenancePolicy.none())//
                .build().getValue();
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

    private static final class GuardedDirectory implements Directory {

        private final Directory delegate;
        private final AtomicBoolean ioBlocked;

        private GuardedDirectory(final Directory delegate) {
            this(delegate, new AtomicBoolean(false));
        }

        private GuardedDirectory(final Directory delegate,
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
        public FileReader getFileReader(final String fileName) {
            ensureAllowed("getFileReader");
            return delegate.getFileReader(fileName);
        }

        @Override
        public FileReader getFileReader(final String fileName,
                final int bufferSize) {
            ensureAllowed("getFileReader(buffer)");
            return delegate.getFileReader(fileName, bufferSize);
        }

        @Override
        public FileReaderSeekable getFileReaderSeekable(
                final String fileName) {
            ensureAllowed("getFileReaderSeekable");
            return delegate.getFileReaderSeekable(fileName);
        }

        @Override
        public FileWriter getFileWriter(
                final String fileName) {
            ensureAllowed("getFileWriter");
            return delegate.getFileWriter(fileName);
        }

        @Override
        public FileWriter getFileWriter(
                final String fileName, final Directory.Access access) {
            ensureAllowed("getFileWriter(access)");
            return delegate.getFileWriter(fileName, access);
        }

        @Override
        public FileWriter getFileWriter(
                final String fileName, final Directory.Access access,
                final int bufferSize) {
            ensureAllowed("getFileWriter(access,buffer)");
            return delegate.getFileWriter(fileName, access, bufferSize);
        }

        @Override
        public boolean isFileExists(final String fileName) {
            ensureAllowed("isFileExists");
            return delegate.isFileExists(fileName);
        }

        @Override
        public boolean deleteFile(final String fileName) {
            ensureAllowed("deleteFile");
            return delegate.deleteFile(fileName);
        }

        @Override
        public Stream<String> getFileNames() {
            ensureAllowed("getFileNames");
            return delegate.getFileNames();
        }

        @Override
        public void renameFile(final String currentFileName,
                final String newFileName) {
            ensureAllowed("renameFile");
            delegate.renameFile(currentFileName, newFileName);
        }

        @Override
        public Directory openSubDirectory(
                final String directoryName) {
            ensureAllowed("openSubDirectory");
            return new GuardedDirectory(delegate.openSubDirectory(directoryName),
                    ioBlocked);
        }

        @Override
        public boolean mkdir(final String directoryName) {
            ensureAllowed("mkdir");
            return delegate.mkdir(directoryName);
        }

        @Override
        public boolean rmdir(final String directoryName) {
            ensureAllowed("rmdir");
            return delegate.rmdir(directoryName);
        }

        @Override
        public FileLock getLock(final String fileName) {
            ensureAllowed("getLock");
            return delegate.getLock(fileName);
        }

        private void ensureAllowed(final String operation) {
            if (ioBlocked.get()) {
                throw new IllegalStateException(
                        "IO blocked during publish: " + operation);
            }
        }
    }
}

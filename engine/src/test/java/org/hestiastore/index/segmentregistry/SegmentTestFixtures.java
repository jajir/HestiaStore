package org.hestiastore.index.segmentregistry;

import org.hestiastore.index.directory.MemDirectory;

public final class SegmentTestFixtures {

    public static final String SEGMENT_DIR_NAME = "segment-00001";

    private SegmentTestFixtures() {
    }

    public static final class FailingRootDeleteDirectory extends MemDirectory {
        private final String failingDirectoryName;

        public FailingRootDeleteDirectory(final String failingDirectoryName) {
            this.failingDirectoryName = failingDirectoryName;
        }

        @Override
        public boolean rmdir(final String directoryName) {
            if (failingDirectoryName.equals(directoryName)) {
                throw new IllegalStateException(
                        "Simulated root delete failure.");
            }
            return super.rmdir(directoryName);
        }
    }
}

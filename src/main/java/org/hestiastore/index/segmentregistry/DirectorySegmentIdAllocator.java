package org.hestiastore.index.segmentregistry;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segment.SegmentId;

/**
 * Segment id allocator that scans existing segment directories and allocates
 * ids above the current maximum.
 */
public final class DirectorySegmentIdAllocator implements SegmentIdAllocator {

    private static final Pattern SEGMENT_DIR_PATTERN = Pattern
            .compile("^segment-(\\d{5})$");
    private static final int DEFAULT_FIRST_ID = 1;

    private final AtomicInteger nextId;

    /**
     * Creates an allocator that scans the provided directory for segment
     * directories.
     *
     * @param directoryFacade directory facade
     */
    public DirectorySegmentIdAllocator(final Directory directoryFacade) {
        Vldtn.requireNonNull(directoryFacade, "directoryFacade");
        final int startId = resolveStartId(directoryFacade, DEFAULT_FIRST_ID);
        this.nextId = new AtomicInteger(startId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SegmentId nextId() {
        return SegmentId.of(nextId.getAndIncrement());
    }

    private static int resolveStartId(final Directory directoryFacade,
            final int defaultFirstId) {
        int maxId = -1;
        try (Stream<String> names = directoryFacade.getFileNames()) {
            maxId = names.map(DirectorySegmentIdAllocator::parseSegmentId)
                    .filter(id -> id >= 0)
                    .max(Integer::compareTo)
                    .orElse(-1);
        }
        final int candidate = maxId + 1;
        return Math.max(candidate, defaultFirstId);
    }

    private static int parseSegmentId(final String name) {
        if (name == null || name.isBlank()) {
            return -1;
        }
        final Matcher matcher = SEGMENT_DIR_PATTERN.matcher(name);
        if (!matcher.matches()) {
            return -1;
        }
        try {
            return Integer.parseInt(matcher.group(1));
        } catch (final NumberFormatException e) {
            return -1;
        }
    }
}

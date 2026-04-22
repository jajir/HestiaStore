package org.hestiastore.index.segmentindex.core.storage;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segment.SegmentDirectoryLayout;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;

/**
 * Inspects recovery-time segment directories for orphaned data and stale lock
 * files.
 *
 * @param <K> key type
 */
final class RecoverySegmentDirectoryInspector<K> {

    private static final Pattern SEGMENT_DIRECTORY_PATTERN = Pattern
            .compile("^segment-(\\d{5})$");
    private static final int BOOTSTRAP_SEGMENT_ID = 0;

    private final Directory directoryFacade;
    private final KeyToSegmentMap<K> keyToSegmentMap;

    RecoverySegmentDirectoryInspector(final Directory directoryFacade,
            final KeyToSegmentMap<K> keyToSegmentMap) {
        this.directoryFacade = Vldtn.requireNonNull(directoryFacade,
                "directoryFacade");
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
    }

    List<SegmentId> discoverOrphanedSegmentDirectories() {
        final Set<SegmentId> mappedSegmentIds = mappedSegmentIds();
        final List<SegmentId> orphanedSegmentIds = new ArrayList<>();
        try (var fileNames = directoryFacade.getFileNames()) {
            fileNames.forEach(name -> collectOrphanedSegmentId(name,
                    mappedSegmentIds, orphanedSegmentIds));
        }
        return orphanedSegmentIds;
    }

    boolean hasSegmentLockFile(final SegmentId segmentId) {
        final String segmentDirectoryName = segmentId.getName();
        if (!directoryFacade.isFileExists(segmentDirectoryName)) {
            return false;
        }
        final Directory segmentDirectory = directoryFacade
                .openSubDirectory(segmentDirectoryName);
        final String lockFileName = new SegmentDirectoryLayout(segmentId)
                .getLockFileName();
        return segmentDirectory.isFileExists(lockFileName);
    }

    private Set<SegmentId> mappedSegmentIds() {
        return new HashSet<>(keyToSegmentMap.getSegmentIds());
    }

    private void collectOrphanedSegmentId(final String fileName,
            final Set<SegmentId> mappedSegmentIds,
            final List<SegmentId> orphanedSegmentIds) {
        final SegmentId segmentId = parseSegmentDirectoryName(fileName);
        if (!isOrphanedSegmentDirectory(segmentId, mappedSegmentIds)) {
            return;
        }
        orphanedSegmentIds.add(segmentId);
    }

    private SegmentId parseSegmentDirectoryName(final String name) {
        if (name == null) {
            return null;
        }
        final var matcher = SEGMENT_DIRECTORY_PATTERN.matcher(name);
        if (!matcher.matches()) {
            return null;
        }
        try {
            return SegmentId.of(Integer.parseInt(matcher.group(1)));
        } catch (final NumberFormatException e) {
            return null;
        }
    }

    private boolean isOrphanedSegmentDirectory(final SegmentId segmentId,
            final Set<SegmentId> mappedSegmentIds) {
        return segmentId != null
                && segmentId.getId() != BOOTSTRAP_SEGMENT_ID
                && !mappedSegmentIds.contains(segmentId);
    }
}

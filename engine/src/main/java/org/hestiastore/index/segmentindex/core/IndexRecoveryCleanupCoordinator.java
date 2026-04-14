package org.hestiastore.index.segmentindex.core;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segment.SegmentDirectoryLayout;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.hestiastore.index.segmentregistry.SegmentRegistryResultStatus;
import org.slf4j.Logger;

/**
 * Owns recovery-time cleanup of orphaned segment directories and stale lock
 * probing for consistency checks.
 */
final class IndexRecoveryCleanupCoordinator<K, V> {

    private static final String OPERATION_CLEANUP_ORPHAN_SEGMENT = "cleanupOrphanSegment";
    private static final Pattern SEGMENT_DIRECTORY_PATTERN = Pattern
            .compile("^segment-(\\d{5})$");
    private static final int BOOTSTRAP_SEGMENT_ID = 0;

    private final Logger logger;
    private final Directory directoryFacade;
    private final KeyToSegmentMap<K> keyToSegmentMap;
    private final SegmentRegistry<K, V> segmentRegistry;
    private final IndexRetryPolicy retryPolicy;

    IndexRecoveryCleanupCoordinator(final Logger logger,
            final Directory directoryFacade,
            final KeyToSegmentMap<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final IndexRetryPolicy retryPolicy) {
        this.logger = Vldtn.requireNonNull(logger, "logger");
        this.directoryFacade = Vldtn.requireNonNull(directoryFacade,
                "directoryFacade");
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
        this.retryPolicy = Vldtn.requireNonNull(retryPolicy, "retryPolicy");
    }

    void cleanupOrphanedSegmentDirectories() {
        final Set<SegmentId> mappedSegmentIds = new HashSet<>(
                keyToSegmentMap.getSegmentIds());
        final List<SegmentId> orphanedSegmentIds = new ArrayList<>();
        try (var fileNames = directoryFacade.getFileNames()) {
            fileNames.forEach(name -> {
                final SegmentId segmentId = parseSegmentDirectoryName(name);
                if (segmentId != null
                        && segmentId.getId() != BOOTSTRAP_SEGMENT_ID
                        && !mappedSegmentIds.contains(segmentId)) {
                    orphanedSegmentIds.add(segmentId);
                }
            });
        }
        orphanedSegmentIds.forEach(this::deleteOrphanedSegmentDirectory);
    }

    boolean hasSegmentLockFile(final SegmentId segmentId) {
        final SegmentId nonNullSegmentId = Vldtn.requireNonNull(segmentId,
                "segmentId");
        final String segmentDirectoryName = nonNullSegmentId.getName();
        if (!directoryFacade.isFileExists(segmentDirectoryName)) {
            return false;
        }
        final Directory segmentDirectory = directoryFacade
                .openSubDirectory(segmentDirectoryName);
        final String lockFileName = new SegmentDirectoryLayout(nonNullSegmentId)
                .getLockFileName();
        return segmentDirectory.isFileExists(lockFileName);
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

    private void deleteOrphanedSegmentDirectory(final SegmentId segmentId) {
        final long startNanos = retryPolicy.startNanos();
        SegmentRegistryResultStatus status = segmentRegistry
                .deleteSegment(segmentId).getStatus();
        while (status == SegmentRegistryResultStatus.BUSY) {
            try {
                retryPolicy.backoffOrThrow(startNanos,
                        OPERATION_CLEANUP_ORPHAN_SEGMENT, segmentId);
            } catch (final IndexException timeout) {
                logger.warn(
                        "Orphaned segment directory '{}' could not be deleted because cleanup timed out.",
                        segmentId);
                return;
            }
            status = segmentRegistry.deleteSegment(segmentId).getStatus();
        }
        if (status == SegmentRegistryResultStatus.OK
                || status == SegmentRegistryResultStatus.CLOSED) {
            logger.info(
                    "Deleted orphaned segment directory '{}' during recovery/consistency cleanup.",
                    segmentId);
        } else {
            logger.warn(
                    "Orphaned segment directory '{}' could not be deleted during cleanup: {}",
                    segmentId, status);
        }
    }
}

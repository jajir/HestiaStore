package org.hestiastore.index.segmentindex.wal;

import java.util.List;
import java.util.function.Consumer;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.segmentindex.configuration.api.IndexWalConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Replays valid WAL records and repairs invalid tails according to policy.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class WalRecoveryManager<K, V> {

    private static final int BUFFER_SIZE = 8 * 1024;
    private static final Logger LOGGER = LoggerFactory
            .getLogger(WalRecoveryManager.class);

    private final IndexWalConfiguration wal;
    private final WalStorage storage;
    private final WalMetadataCatalog metadataCatalog;
    private final WalRecordCodec<K, V> recordCodec;
    private final WalSegmentCatalog segmentCatalog;
    private final WalRuntimeMetrics metrics;

    WalRecoveryManager(final IndexWalConfiguration wal,
            final WalStorage storage, final WalMetadataCatalog metadataCatalog,
            final WalRecordCodec<K, V> recordCodec,
            final WalSegmentCatalog segmentCatalog,
            final WalRuntimeMetrics metrics) {
        this.wal = wal;
        this.storage = storage;
        this.metadataCatalog = metadataCatalog;
        this.recordCodec = recordCodec;
        this.segmentCatalog = segmentCatalog;
        this.metrics = metrics;
    }

    /**
     * Recovers all valid records after the persisted checkpoint.
     *
     * @param replayConsumer recovered-record consumer
     * @return recovery outcome
     */
    WalRecoveryOutcome recover(
            final Consumer<WalRuntime.ReplayRecord<K, V>> replayConsumer) {
        metadataCatalog.ensureFormatMarker();
        long checkpointLsn = metadataCatalog.readCheckpointLsn();
        segmentCatalog.resetRecoveredSegments();
        final List<WalSegmentDescriptor> discoveredSegments = metadataCatalog
                .discoverSegmentsStrict();
        LOGGER.info(
                "event=wal_recovery_start checkpointLsn={} segmentCount={} corruptionPolicy={}",
                checkpointLsn, discoveredSegments.size(),
                wal.getCorruptionPolicy());
        boolean truncatedTail = false;
        long maxLsn = checkpointLsn;
        long lastReplayedLsn = checkpointLsn;
        long lastSeenLsn = 0L;
        for (int i = 0; i < discoveredSegments.size(); i++) {
            final WalSegmentDescriptor current = discoveredSegments.get(i);
            final ScanResult scan = scanAndReplaySegment(current.name(),
                    checkpointLsn, lastSeenLsn, replayConsumer);
            maxLsn = Math.max(maxLsn, scan.maxLsn());
            lastReplayedLsn = Math.max(lastReplayedLsn,
                    scan.lastReplayedLsn());
            lastSeenLsn = Math.max(lastSeenLsn, scan.lastSeenLsn());
            if (scan.invalidTail()) {
                truncatedTail = true;
                LOGGER.warn(
                        "event=wal_recovery_invalid_tail segment={} validBytes={} observedMaxLsn={} policy={}",
                        current.name(), scan.validBytes(), scan.maxLsn(),
                        wal.getCorruptionPolicy());
                handleInvalidTail(current.name(), scan.validBytes());
                final int deletedAfterCorruption = segmentCatalog
                        .deleteSegmentsAfter(discoveredSegments, i + 1);
                if (deletedAfterCorruption > 0) {
                    LOGGER.warn(
                            "event=wal_recovery_drop_newer_segments deletedSegments={} fromSegmentIndex={}",
                            deletedAfterCorruption, i + 1);
                }
                addRecoveredSegment(current, scan);
                break;
            }
            if (scan.validBytes() > 0L) {
                addRecoveredSegment(current, scan);
            } else {
                final String name = current.name();
                storage.delete(name);
                storage.syncMetadata();
                LOGGER.info("event=wal_recovery_drop_empty_segment segment={}",
                        name);
            }
        }
        if (lastSeenLsn > 0L && checkpointLsn > lastSeenLsn) {
            final long previousCheckpointLsn = checkpointLsn;
            checkpointLsn = lastSeenLsn;
            metadataCatalog.writeCheckpointLsnAtomic(checkpointLsn);
            maxLsn = lastSeenLsn;
            LOGGER.warn(
                    "event=wal_recovery_checkpoint_clamp previousCheckpointLsn={} clampedCheckpointLsn={} maxLsn={}",
                    previousCheckpointLsn, checkpointLsn, maxLsn);
        }
        lastReplayedLsn = Math.min(lastReplayedLsn, maxLsn);
        LOGGER.info(
                "event=wal_recovery_complete maxLsn={} checkpointLsn={} lastReplayedLsn={} truncatedTail={} segmentCount={}",
                maxLsn, checkpointLsn, lastReplayedLsn, truncatedTail,
                segmentCatalog.segmentCount());
        return new WalRecoveryOutcome(checkpointLsn, lastReplayedLsn, maxLsn,
                truncatedTail);
    }

    private ScanResult scanAndReplaySegment(final String fileName,
            final long replayAfterLsn, final long minimumLsnExclusive,
            final Consumer<WalRuntime.ReplayRecord<K, V>> replayConsumer) {
        long offset = 0L;
        long validOffset = 0L;
        long maxLsn = 0L;
        long lastReplayedLsn = replayAfterLsn;
        long previousLsn = minimumLsnExclusive;
        final byte[] lenBytes = new byte[4];
        while (true) {
            final int lenRead = readFullyAllowEof(fileName, offset, lenBytes, 0,
                    4);
            if (lenRead == -1) {
                break;
            }
            if (lenRead != 4) {
                return new ScanResult(validOffset, maxLsn, lastReplayedLsn,
                        previousLsn, true);
            }
            final int bodyLen = WalRecordCodec.readInt(lenBytes, 0);
            if (!recordCodec.isBodyLengthValid(bodyLen)) {
                return new ScanResult(validOffset, maxLsn, lastReplayedLsn,
                        previousLsn, true);
            }
            final byte[] body = new byte[bodyLen];
            if (readFullyAllowEof(fileName, offset + 4L, body, 0,
                    bodyLen) != bodyLen) {
                return new ScanResult(validOffset, maxLsn, lastReplayedLsn,
                        previousLsn, true);
            }
            final WalDecodedRecord<K, V> decoded;
            try {
                decoded = recordCodec.decodeBody(body, previousLsn);
            } catch (RuntimeException ex) {
                return new ScanResult(validOffset, maxLsn, lastReplayedLsn,
                        previousLsn, true);
            }
            offset += 4L + bodyLen;
            validOffset = offset;
            previousLsn = decoded.lsn();
            maxLsn = Math.max(maxLsn, decoded.lsn());
            if (decoded.lsn() > replayAfterLsn) {
                replayConsumer.accept(new WalRuntime.ReplayRecord<>(decoded.lsn(),
                        decoded.operation(), decoded.key(), decoded.value()));
                lastReplayedLsn = decoded.lsn();
            }
        }
        return new ScanResult(validOffset, maxLsn, lastReplayedLsn, previousLsn,
                false);
    }

    private void handleInvalidTail(final String fileName, final long validBytes) {
        metrics.recordCorruption();
        if (wal.isFailFastCorruptionPolicy()) {
            LOGGER.error(
                    "event=wal_recovery_tail_repair action=fail_fast segment={} validBytes={}",
                    fileName, validBytes);
            throw new IndexException(
                    String.format("WAL corruption detected in '%s'.", fileName));
        }
        metrics.recordTruncation();
        if (validBytes <= 0L) {
            storage.delete(fileName);
            storage.syncMetadata();
            LOGGER.warn(
                    "event=wal_recovery_tail_repair action=delete_segment segment={} validBytes={}",
                    fileName, validBytes);
            return;
        }
        storage.truncate(fileName, validBytes);
        storage.sync(fileName);
        LOGGER.warn(
                "event=wal_recovery_tail_repair action=truncate_segment segment={} validBytes={}",
                fileName, validBytes);
    }

    private void addRecoveredSegment(final WalSegmentDescriptor segment,
            final ScanResult scan) {
        if (scan.validBytes() <= 0L) {
            return;
        }
        final long segmentMaxLsn = scan.maxLsn() > 0L ? scan.maxLsn()
                : segment.baseLsn();
        segmentCatalog.addRecoveredSegment(segment.name(), segment.baseLsn(),
                scan.validBytes(), segmentMaxLsn);
    }

    private int readFullyAllowEof(final String fileName, final long position,
            final byte[] destination, final int offset, final int length) {
        int totalRead = 0;
        long currentPosition = position;
        while (totalRead < length) {
            final int read = storage.read(fileName, currentPosition, destination,
                    offset + totalRead, Math.min(length - totalRead, BUFFER_SIZE));
            if (read < 0) {
                return totalRead == 0 ? -1 : totalRead;
            }
            if (read == 0) {
                continue;
            }
            totalRead += read;
            currentPosition += read;
        }
        return totalRead;
    }

    private static final class ScanResult {
        private final long validBytes;
        private final long maxLsn;
        private final long lastReplayedLsn;
        private final long lastSeenLsn;
        private final boolean invalidTail;

        ScanResult(final long validBytes, final long maxLsn,
                final long lastReplayedLsn, final long lastSeenLsn,
                final boolean invalidTail) {
            this.validBytes = validBytes;
            this.maxLsn = maxLsn;
            this.lastReplayedLsn = lastReplayedLsn;
            this.lastSeenLsn = lastSeenLsn;
            this.invalidTail = invalidTail;
        }

        long validBytes() {
            return validBytes;
        }

        long maxLsn() {
            return maxLsn;
        }

        long lastReplayedLsn() {
            return lastReplayedLsn;
        }

        long lastSeenLsn() {
            return lastSeenLsn;
        }

        boolean invalidTail() {
            return invalidTail;
        }
    }
}

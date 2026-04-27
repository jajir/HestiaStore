package org.hestiastore.index.segmentindex.wal;

import java.util.List;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.segmentindex.Wal;
import org.hestiastore.index.segmentindex.WalCorruptionPolicy;
import org.slf4j.Logger;

final class WalRecoveryManager<K, V> {

    private static final int BUFFER_SIZE = 8 * 1024;

    private final Wal wal;
    private final WalStorage storage;
    private final WalMetadataCatalog metadataCatalog;
    private final WalRecordCodec<K, V> recordCodec;
    private final WalSegmentCatalog segmentCatalog;
    private final WalRuntimeMetrics metrics;
    private final Logger logger;

    WalRecoveryManager(final Wal wal, final WalStorage storage,
            final WalMetadataCatalog metadataCatalog,
            final WalRecordCodec<K, V> recordCodec,
            final WalSegmentCatalog segmentCatalog,
            final WalRuntimeMetrics metrics, final Logger logger) {
        this.wal = wal;
        this.storage = storage;
        this.metadataCatalog = metadataCatalog;
        this.recordCodec = recordCodec;
        this.segmentCatalog = segmentCatalog;
        this.metrics = metrics;
        this.logger = logger;
    }

    WalRecoveryOutcome recover(
            final WalRuntime.ReplayConsumer<K, V> replayConsumer) {
        final WalCatalogView catalogView = metadataCatalog.loadRecoveryCatalog();
        long checkpointLsn = catalogView.checkpointLsn();
        segmentCatalog.resetRecoveredSegments();
        final List<WalSegmentDescriptor> discoveredSegments = catalogView
                .discoveredSegments();
        logger.info(
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
            if (scan.maxLsn() > maxLsn) {
                maxLsn = scan.maxLsn();
            }
            if (scan.lastReplayedLsn() > lastReplayedLsn) {
                lastReplayedLsn = scan.lastReplayedLsn();
            }
            if (scan.lastSeenLsn() > lastSeenLsn) {
                lastSeenLsn = scan.lastSeenLsn();
            }
            if (scan.invalidTail()) {
                truncatedTail = true;
                logger.warn(
                        "event=wal_recovery_invalid_tail segment={} validBytes={} observedMaxLsn={} policy={}",
                        current.name(), scan.validBytes(), scan.maxLsn(),
                        wal.getCorruptionPolicy());
                handleInvalidTail(current.name(), scan.validBytes());
                final int deletedAfterCorruption = segmentCatalog
                        .deleteSegmentsAfter(discoveredSegments, i + 1);
                if (deletedAfterCorruption > 0) {
                    logger.warn(
                            "event=wal_recovery_drop_newer_segments deletedSegments={} fromSegmentIndex={}",
                            deletedAfterCorruption, i + 1);
                }
                if (scan.validBytes() > 0L) {
                    final long segmentMaxLsn = scan.maxLsn() > 0L
                            ? scan.maxLsn()
                            : current.baseLsn();
                    segmentCatalog.addRecoveredSegment(current.name(),
                            current.baseLsn(), scan.validBytes(),
                            segmentMaxLsn);
                }
                break;
            }
            if (scan.validBytes() > 0L) {
                final long segmentMaxLsn = scan.maxLsn() > 0L ? scan.maxLsn()
                        : current.baseLsn();
                segmentCatalog.addRecoveredSegment(current.name(),
                        current.baseLsn(), scan.validBytes(), segmentMaxLsn);
            } else {
                final String name = current.name();
                storage.delete(name);
                storage.syncMetadata();
                logger.info("event=wal_recovery_drop_empty_segment segment={}",
                        name);
            }
        }
        if (lastSeenLsn > 0L && checkpointLsn > lastSeenLsn) {
            final long previousCheckpointLsn = checkpointLsn;
            checkpointLsn = lastSeenLsn;
            metadataCatalog.writeCheckpointLsnAtomic(checkpointLsn);
            maxLsn = lastSeenLsn;
            logger.warn(
                    "event=wal_recovery_checkpoint_clamp previousCheckpointLsn={} clampedCheckpointLsn={} maxLsn={}",
                    previousCheckpointLsn, checkpointLsn, maxLsn);
        }
        if (lastReplayedLsn > maxLsn) {
            lastReplayedLsn = maxLsn;
        }
        logger.info(
                "event=wal_recovery_complete maxLsn={} checkpointLsn={} lastReplayedLsn={} truncatedTail={} segmentCount={}",
                maxLsn, checkpointLsn, lastReplayedLsn, truncatedTail,
                segmentCatalog.segmentCount());
        return new WalRecoveryOutcome(checkpointLsn, lastReplayedLsn, maxLsn,
                truncatedTail);
    }

    private ScanResult scanAndReplaySegment(final String fileName,
            final long replayAfterLsn, final long minimumLsnExclusive,
            final WalRuntime.ReplayConsumer<K, V> replayConsumer) {
        long offset = 0L;
        long validOffset = 0L;
        long maxLsn = 0L;
        long lastReplayedLsn = replayAfterLsn;
        long previousLsn = minimumLsnExclusive;
        while (true) {
            final byte[] lenBytes = new byte[4];
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
            if (!readFully(fileName, offset + 4L, body, 0, bodyLen)) {
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
            if (decoded.lsn() > maxLsn) {
                maxLsn = decoded.lsn();
            }
            if (decoded.lsn() > replayAfterLsn) {
                replayConsumer.accept(new WalRuntime.ReplayRecord<>(decoded.lsn(),
                        decoded.operation(), decoded.key(), decoded.value()));
                if (decoded.lsn() > lastReplayedLsn) {
                    lastReplayedLsn = decoded.lsn();
                }
            }
        }
        return new ScanResult(validOffset, maxLsn, lastReplayedLsn, previousLsn,
                false);
    }

    private void handleInvalidTail(final String fileName, final long validBytes) {
        metrics.recordCorruption();
        if (wal.getCorruptionPolicy() == WalCorruptionPolicy.FAIL_FAST) {
            logger.error(
                    "event=wal_recovery_tail_repair action=fail_fast segment={} validBytes={}",
                    fileName, validBytes);
            throw new IndexException(
                    String.format("WAL corruption detected in '%s'.", fileName));
        }
        metrics.recordTruncation();
        if (validBytes <= 0L) {
            storage.delete(fileName);
            storage.syncMetadata();
            logger.warn(
                    "event=wal_recovery_tail_repair action=delete_segment segment={} validBytes={}",
                    fileName, validBytes);
            return;
        }
        storage.truncate(fileName, validBytes);
        storage.sync(fileName);
        logger.warn(
                "event=wal_recovery_tail_repair action=truncate_segment segment={} validBytes={}",
                fileName, validBytes);
    }

    private int readFullyAllowEof(final String fileName, final long position,
            final byte[] destination, final int offset, final int length) {
        int totalRead = 0;
        long currentPosition = position;
        while (totalRead < length) {
            final int read = storage.read(fileName, currentPosition, destination,
                    offset + totalRead, length - totalRead);
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

    private boolean readFully(final String fileName, final long position,
            final byte[] destination, final int offset, final int length) {
        int totalRead = 0;
        long currentPosition = position;
        while (totalRead < length) {
            final int read = storage.read(fileName, currentPosition, destination,
                    offset + totalRead, Math.min(length - totalRead, BUFFER_SIZE));
            if (read < 0) {
                return false;
            }
            if (read == 0) {
                continue;
            }
            totalRead += read;
            currentPosition += read;
        }
        return true;
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

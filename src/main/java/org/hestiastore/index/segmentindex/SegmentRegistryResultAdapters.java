package org.hestiastore.index.segmentindex;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentResult;
import org.hestiastore.index.segment.SegmentResultStatus;

/**
 * Converters between registry results and segment results for staged migration.
 */
public final class SegmentRegistryResultAdapters {

    private SegmentRegistryResultAdapters() {
    }

    public static <T> SegmentRegistryResult<T> fromSegmentResult(
            final SegmentResult<T> result) {
        Vldtn.requireNonNull(result, "result");
        switch (result.getStatus()) {
        case OK:
            return SegmentRegistryResult.ok(result.getValue());
        case BUSY:
            return SegmentRegistryResult.busy();
        case CLOSED:
            return SegmentRegistryResult.closed();
        case ERROR:
            return SegmentRegistryResult.error();
        default:
            throw new IllegalArgumentException(
                    "Unsupported status: " + result.getStatus());
        }
    }

    public static SegmentRegistryResultStatus fromSegmentStatus(
            final SegmentResultStatus status) {
        Vldtn.requireNonNull(status, "status");
        switch (status) {
        case OK:
            return SegmentRegistryResultStatus.OK;
        case BUSY:
            return SegmentRegistryResultStatus.BUSY;
        case CLOSED:
            return SegmentRegistryResultStatus.CLOSED;
        case ERROR:
            return SegmentRegistryResultStatus.ERROR;
        default:
            throw new IllegalArgumentException(
                    "Unsupported status: " + status);
        }
    }

    public static <T> SegmentResult<T> toSegmentResult(
            final SegmentRegistryResult<T> result) {
        Vldtn.requireNonNull(result, "result");
        switch (result.getStatus()) {
        case OK:
            return SegmentResult.ok(result.getValue());
        case BUSY:
            return SegmentResult.busy();
        case CLOSED:
            return SegmentResult.closed();
        case ERROR:
            return SegmentResult.error();
        default:
            throw new IllegalArgumentException(
                    "Unsupported status: " + result.getStatus());
        }
    }

    public static SegmentResultStatus toSegmentStatus(
            final SegmentRegistryResultStatus status) {
        Vldtn.requireNonNull(status, "status");
        switch (status) {
        case OK:
            return SegmentResultStatus.OK;
        case BUSY:
            return SegmentResultStatus.BUSY;
        case CLOSED:
            return SegmentResultStatus.CLOSED;
        case ERROR:
            return SegmentResultStatus.ERROR;
        default:
            throw new IllegalArgumentException(
                    "Unsupported status: " + status);
        }
    }
}

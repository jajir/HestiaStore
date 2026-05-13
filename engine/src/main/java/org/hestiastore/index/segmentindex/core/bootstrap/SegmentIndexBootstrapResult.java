package org.hestiastore.index.segmentindex.core.bootstrap;

import java.util.Optional;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.session.IndexInternal;

/**
 * Result of one segment-index bootstrap run.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SegmentIndexBootstrapResult<K, V> {

    private final SegmentIndexBootstrapStatus status;

    private final IndexInternal<K, V> index;

    private SegmentIndexBootstrapResult(
            final SegmentIndexBootstrapStatus status,
            final IndexInternal<K, V> index) {
        this.status = Vldtn.requireNonNull(status, "status");
        this.index = validateIndex(status, index);
    }

    static <K, V> SegmentIndexBootstrapResult<K, V> created(
            final IndexInternal<K, V> index) {
        return new SegmentIndexBootstrapResult<>(
                SegmentIndexBootstrapStatus.CREATED, index);
    }

    static <K, V> SegmentIndexBootstrapResult<K, V> opened(
            final IndexInternal<K, V> index) {
        return new SegmentIndexBootstrapResult<>(
                SegmentIndexBootstrapStatus.OPENED, index);
    }

    static <K, V> SegmentIndexBootstrapResult<K, V> notFound() {
        return new SegmentIndexBootstrapResult<>(
                SegmentIndexBootstrapStatus.NOT_FOUND, null);
    }

    SegmentIndexBootstrapStatus status() {
        return status;
    }

    Optional<IndexInternal<K, V>> index() {
        return Optional.ofNullable(index);
    }

    IndexInternal<K, V> requireIndex() {
        if (index == null) {
            throw new IllegalStateException(
                    "Bootstrap result does not contain an index.");
        }
        return index;
    }

    private static <K, V> IndexInternal<K, V> validateIndex(
            final SegmentIndexBootstrapStatus status,
            final IndexInternal<K, V> index) {
        if (status == SegmentIndexBootstrapStatus.CREATED
                || status == SegmentIndexBootstrapStatus.OPENED) {
            return Vldtn.requireNonNull(index, "index");
        }
        if (index != null) {
            throw new IllegalArgumentException(
                    "index must be null for " + status + " status.");
        }
        return null;
    }
}

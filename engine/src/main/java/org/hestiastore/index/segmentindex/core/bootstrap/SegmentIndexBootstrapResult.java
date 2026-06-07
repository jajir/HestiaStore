package org.hestiastore.index.segmentindex.core.bootstrap;

import java.util.Optional;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexResourceClosingAdapter;

/**
 * Result of one segment-index bootstrap run.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SegmentIndexBootstrapResult<K, V> {

    private final SegmentIndexBootstrapStatus status;

    private final SegmentIndexResourceClosingAdapter<K, V> index;

    private SegmentIndexBootstrapResult(
            final SegmentIndexBootstrapStatus status,
            final SegmentIndexResourceClosingAdapter<K, V> index) {
        this.status = Vldtn.requireNonNull(status, "status");
        this.index = validateIndex(status, index);
    }

    static <K, V> SegmentIndexBootstrapResult<K, V> created(
            final SegmentIndexResourceClosingAdapter<K, V> index) {
        return new SegmentIndexBootstrapResult<>(
                SegmentIndexBootstrapStatus.CREATED, index);
    }

    static <K, V> SegmentIndexBootstrapResult<K, V> opened(
            final SegmentIndexResourceClosingAdapter<K, V> index) {
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

    Optional<SegmentIndexResourceClosingAdapter<K, V>> index() {
        return Optional.ofNullable(index);
    }

    SegmentIndexResourceClosingAdapter<K, V> requireIndex() {
        if (index == null) {
            throw new IllegalStateException(
                    "Bootstrap result does not contain an index.");
        }
        return index;
    }

    private static <K, V> SegmentIndexResourceClosingAdapter<K, V> validateIndex(
            final SegmentIndexBootstrapStatus status,
            final SegmentIndexResourceClosingAdapter<K, V> index) {
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

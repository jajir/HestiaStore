package org.hestiastore.index.segmentindex.core.session;

import java.lang.reflect.Field;

import org.hestiastore.index.segmentindex.core.session.state.SegmentIndexStateCoordinator;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.wal.WalRuntime;

/**
 * Test-only bridge exposing package-private runtime collaborators.
 */
public final class SegmentIndexTestAccess {

    private SegmentIndexTestAccess() {
    }

    @SuppressWarnings("unchecked")
    public static <K> KeyToSegmentMap<K> keyToSegmentMap(final Object index) {
        return (KeyToSegmentMap<K>) SegmentIndexRuntimeTestAccess
                .keyToSegmentMap(unwrap(index).runtime());
    }

    public static Object segmentRegistry(final Object index) {
        return SegmentIndexRuntimeTestAccess.segmentRegistry(unwrap(index)
                .runtime());
    }

    public static WalRuntime<?, ?> walRuntime(final Object index) {
        return SegmentIndexRuntimeTestAccess.walRuntime(unwrap(index)
                .runtime());
    }

    public static SegmentIndexStateCoordinator stateCoordinator(final Object index) {
        return new SegmentIndexStateCoordinator(unwrap(index)
                .stateCoordinator());
    }

    private static SegmentIndexImpl<?, ?> unwrap(final Object index) {
        Object current = index;
        while (!(current instanceof SegmentIndexImpl<?, ?>)) {
            try {
                final Field delegateField = current.getClass().getDeclaredField(
                        "delegate");
                delegateField.setAccessible(true);
                current = delegateField.get(current).orElse(null);
            } catch (final ReflectiveOperationException ex) {
                throw new IllegalStateException(
                        "Unable to unwrap segment index for test access", ex);
            }
        }
        return (SegmentIndexImpl<?, ?>) current;
    }
}

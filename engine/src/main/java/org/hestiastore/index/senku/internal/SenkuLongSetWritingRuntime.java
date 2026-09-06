package org.hestiastore.index.senku.internal;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.NullValue;
import org.hestiastore.index.senku.SenkuLongSetWriting;
import org.hestiastore.index.senku.SenkuReady;

/**
 * Typed primitive entry point delegating lifecycle ownership to one runtime.
 */
final class SenkuLongSetWritingRuntime implements SenkuLongSetWriting {

    private final SenkuWritingRuntime<Long, NullValue> runtime;

    /**
     * Creates a primitive view without introducing another lifecycle owner.
     *
     * @param runtime underlying set-configured writing runtime
     */
    SenkuLongSetWritingRuntime(
            final SenkuWritingRuntime<Long, NullValue> runtime) {
        this.runtime = Vldtn.requireNonNull(runtime, "runtime");
    }

    /** {@inheritDoc} */
    @Override
    public void putLong(final long key) {
        runtime.putLong(key);
    }

    /** {@inheritDoc} */
    @Override
    public SenkuReady<Long, NullValue> finishWriting() {
        return runtime.finishWriting();
    }
}

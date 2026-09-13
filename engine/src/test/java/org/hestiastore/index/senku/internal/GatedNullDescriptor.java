package org.hestiastore.index.senku.internal;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.datatype.NullValue;
import org.hestiastore.index.datatype.TypeDescriptorNull;
import org.hestiastore.index.datatype.TypeWriter;

/** Test serializer that gates the first value of each independent page. */
final class GatedNullDescriptor extends TypeDescriptorNull {
    private final CountDownLatch entered;
    private final CountDownLatch release;

    GatedNullDescriptor(final CountDownLatch entered,
            final CountDownLatch release) {
        this.entered = entered;
        this.release = release;
    }

    @Override
    public TypeWriter<NullValue> getTypeWriter() {
        final AtomicBoolean firstWrite = new AtomicBoolean(true);
        return (writer, value) -> {
            if (firstWrite.compareAndSet(true, false)) {
                entered.countDown();
                try {
                    if (!release.await(10, TimeUnit.SECONDS)) {
                        throw new IndexException(
                                "Timed out awaiting parallel page encoders");
                    }
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                    throw new IndexException(
                            "Interrupted awaiting parallel page encoders",
                            exception);
                }
            }
            return 0;
        };
    }
}

package org.hestiastore.index.senku.internal;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.datatype.NullValue;
import org.hestiastore.index.senku.SenkuReady;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SenkuLongSetWritingRuntimeTest {

    @Mock
    private SenkuWritingRuntime<Long, NullValue> runtime;
    @Mock
    private SenkuReady<Long, NullValue> ready;

    @Test
    void delegatesPrimitivePutAndReadyOwnershipToExistingRuntime() {
        final SenkuLongSetWritingRuntime writing = new SenkuLongSetWritingRuntime(
                runtime);
        writing.putLong(0L);
        verify(runtime).putLong(0L);
        when(runtime.finishWriting()).thenReturn(ready);
        assertSame(ready, writing.finishWriting());
    }

    @Test
    void rejectsMissingRuntimeAndPreservesDelegatedFailure() {
        assertThrows(IllegalArgumentException.class,
                () -> new SenkuLongSetWritingRuntime(null));
        final IndexException failure = new IndexException("expected");
        doThrow(failure).when(runtime).putLong(1L);
        assertSame(failure, assertThrows(IndexException.class,
                () -> new SenkuLongSetWritingRuntime(runtime).putLong(1L)));
    }
}

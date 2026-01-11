package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class IndexAsyncAdapterTest {

    @Mock
    private SegmentIndex<String, String> delegate;

    private IndexAsyncAdapter<String, String> adapter;

    @BeforeEach
    void setUp() {
        adapter = new IndexAsyncAdapter<>(delegate);
    }

    @AfterEach
    void tearDown() {
        if (adapter != null && !adapter.wasClosed()) {
            adapter.close();
        }
    }

    @Test
    void close_waits_for_inflight_async_operation() throws Exception {
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);

        doAnswer(invocation -> {
            entered.countDown();
            if (!release.await(5, TimeUnit.SECONDS)) {
                throw new TimeoutException("Timed out waiting for release");
            }
            return "value";
        }).when(delegate).get("key");

        final CompletionStage<String> stage = adapter.getAsync("key");

        assertTrue(entered.await(5, TimeUnit.SECONDS),
                "Async operation did not start in time");

        final CompletableFuture<Void> closeFuture = CompletableFuture
                .runAsync(adapter::close);

        assertThrows(TimeoutException.class,
                () -> closeFuture.get(200, TimeUnit.MILLISECONDS),
                "close() returned while async operation was in-flight");

        release.countDown();

        assertEquals("value",
                stage.toCompletableFuture().get(5, TimeUnit.SECONDS));
        closeFuture.get(5, TimeUnit.SECONDS);
        verify(delegate).close();
    }
}

package org.hestiastore.index.segmentindex.core;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;

import org.hestiastore.index.segmentindex.SegmentIndex;
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

    @Test
    void putAsync_delegatesToUnderlyingAsyncMethod() {
        final CompletableFuture<Void> stage = new CompletableFuture<>();
        when(delegate.putAsync("k", "v")).thenReturn(stage);

        assertSame(stage, adapter.putAsync("k", "v"));
    }

    @Test
    void getAsync_delegatesToUnderlyingAsyncMethod() {
        final CompletableFuture<String> stage = new CompletableFuture<>();
        when(delegate.getAsync("k")).thenReturn(stage);

        assertSame(stage, adapter.getAsync("k"));
    }

    @Test
    void deleteAsync_delegatesToUnderlyingAsyncMethod() {
        final CompletableFuture<Void> stage = new CompletableFuture<>();
        when(delegate.deleteAsync("k")).thenReturn(stage);

        assertSame(stage, adapter.deleteAsync("k"));
    }

    @Test
    void close_delegatesToUnderlyingClose() {
        adapter.close();

        verify(delegate).close();
    }
}

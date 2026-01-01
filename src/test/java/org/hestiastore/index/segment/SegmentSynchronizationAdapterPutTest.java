package org.hestiastore.index.segment;

import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentSynchronizationAdapterPutTest {

    @Mock
    private Segment<Integer, String> delegate;

    @Test
    void put_delegates_to_underlying_segment() {
        try (SegmentImplSynchronizationAdapter<Integer, String> adapter = new SegmentImplSynchronizationAdapter<>(
                delegate)) {
            adapter.put(1, "A");
        }

        verify(delegate).put(1, "A");
    }

}

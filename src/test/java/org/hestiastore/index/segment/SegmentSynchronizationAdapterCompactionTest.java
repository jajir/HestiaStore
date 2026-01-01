package org.hestiastore.index.segment;

import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentSynchronizationAdapterCompactionTest {

    @Mock
    private Segment<Integer, String> segment;

    @Test
    void compact_delegates_to_segment() {
        try (SegmentImplSynchronizationAdapter<Integer, String> adapter = new SegmentImplSynchronizationAdapter<>(
                segment)) {
            adapter.compact();
        }

        verify(segment).compact();
        verify(segment).close();
    }
}

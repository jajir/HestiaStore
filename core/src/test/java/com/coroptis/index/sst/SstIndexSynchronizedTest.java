package com.coroptis.index.sst;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class SstIndexSynchronizedTest {

    @Mock
    private SstIndexImpl<Integer, String> index;

    @Test
    public void test_locking_get() throws Exception {
        try (SstIndexSynchronized<Integer, String> synchIndex = new SstIndexSynchronized<>(
                index)) {
            when(index.get(3)).thenReturn("hello");
            assertEquals("hello", synchIndex.get(3));
        }
    }

}

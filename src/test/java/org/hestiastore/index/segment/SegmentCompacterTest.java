package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentCompacterTest {

    @Mock
    private SegmentImpl<Integer, String> segment;

    @Mock
    private VersionController versionController;

    private SegmentCompacter<Integer, String> sc;

    @BeforeEach
    void setUp() {
        sc = new SegmentCompacter<>(versionController);
    }

    @Test
    void test_basic_operations() {
        assertNotNull(sc);
    }

}

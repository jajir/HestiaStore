package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

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
    private SegmentConf segmentConf;

    @Mock
    private VersionController versionController;

    @Mock
    private SegmentPropertiesManager segmentPropertiesManager;

    private SegmentCompacter<Integer, String> sc;

    @BeforeEach
    void setUp() {
        sc = new SegmentCompacter<>(versionController,
                new SegmentCompactionPolicyWithManager(
                        new SegmentCompactionPolicy(segmentConf),
                        segmentPropertiesManager));
    }

    @Test
    void test_basic_operations() {
        assertNotNull(sc);
    }

    @Test
    void test_policy_shouldCompactDuringWriting_yes() {
        when(segmentPropertiesManager.getSegmentStats())
                .thenReturn(new SegmentStats(10, 1000L, 15));
        when(segmentConf.getMaxNumberOfKeysInDeltaCacheDuringWriting())
                .thenReturn(20L);

        SegmentCompactionPolicyWithManager policy = new SegmentCompactionPolicyWithManager(
                new SegmentCompactionPolicy(segmentConf), segmentPropertiesManager);
        assertTrue(policy.shouldCompactDuringWriting(25));
    }

    @Test
    void test_policy_shouldCompactDuringWriting_no() {
        when(segmentPropertiesManager.getSegmentStats())
                .thenReturn(new SegmentStats(10, 1000L, 15));
        when(segmentConf.getMaxNumberOfKeysInDeltaCacheDuringWriting())
                .thenReturn(30L);

        SegmentCompactionPolicyWithManager policy = new SegmentCompactionPolicyWithManager(
                new SegmentCompactionPolicy(segmentConf), segmentPropertiesManager);
        assertFalse(policy.shouldCompactDuringWriting(10));
    }

    // 'shouldBeCompacted' is private now; compaction policy behavior
    // is verified in SegmentCompactionPolicyTest. Here we keep
    // 'shouldBeCompactedDuringWriting' covered above.

}

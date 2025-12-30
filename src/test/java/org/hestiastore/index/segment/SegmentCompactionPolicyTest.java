package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SegmentCompactionPolicyTest {

    private SegmentCompactionPolicy policy;

    @BeforeEach
    void setUp() {
        final SegmentConf segmentConf = new SegmentConf(10, 25, 3, null, null,
                null, 1024, List.of(new ChunkFilterDoNothing()),
                List.of(new ChunkFilterDoNothing()));
        policy = new SegmentCompactionPolicy(segmentConf);
    }

    @Test
    void shouldCompact_returnsTrueWhenDeltaAboveLimit() {
        final SegmentStats stats = new SegmentStats(11, 0, 0);

        assertTrue(policy.shouldCompact(stats));
    }

    @Test
    void shouldCompact_returnsFalseWhenDeltaWithinLimit() {
        final SegmentStats stats = new SegmentStats(10, 0, 0);

        assertFalse(policy.shouldCompact(stats));
    }

    @Test
    void shouldCompactDuringWriting_returnsTrueWhenThresholdExceeded() {
        final SegmentStats stats = new SegmentStats(20, 0, 0);

        assertTrue(policy.shouldCompactDuringWriting(6, 0, stats));
    }

    @Test
    void shouldCompactDuringWriting_returnsTrueWhenDeltaFilesExceeded() {
        final SegmentStats stats = new SegmentStats(0, 0, 0);

        assertTrue(policy.shouldCompactDuringWriting(0,
                SegmentCompactionPolicy.MAX_DELTA_FILES_BEFORE_FORCE_COMPACTION
                        + 1,
                stats));
    }

    @Test
    void shouldCompactDuringWriting_returnsFalseWhenThresholdNotReached() {
        final SegmentStats stats = new SegmentStats(20, 0, 0);

        assertFalse(policy.shouldCompactDuringWriting(5, 0, stats));
    }

    @Test
    void shouldCompactDuringWriting_throwsWhenNegativeKeysProvided() {
        final SegmentStats stats = new SegmentStats(0, 0, 0);

        assertThrows(IllegalArgumentException.class,
                () -> policy.shouldCompactDuringWriting(-1, 0, stats));
    }

    @Test
    void shouldCompactDuringWriting_throwsWhenNegativeDeltaFilesProvided() {
        final SegmentStats stats = new SegmentStats(0, 0, 0);

        assertThrows(IllegalArgumentException.class,
                () -> policy.shouldCompactDuringWriting(0, -1, stats));
    }

    @Test
    void shouldForceCompactionForDeltaFiles_returnsTrueWhenLimitExceeded() {
        assertTrue(policy.shouldForceCompactionForDeltaFiles(
                SegmentCompactionPolicy.MAX_DELTA_FILES_BEFORE_FORCE_COMPACTION
                        + 1));
    }

    @Test
    void shouldForceCompactionForDeltaFiles_returnsFalseWithinLimit() {
        assertFalse(policy.shouldForceCompactionForDeltaFiles(
                SegmentCompactionPolicy.MAX_DELTA_FILES_BEFORE_FORCE_COMPACTION));
    }

    @Test
    void shouldForceCompactionForDeltaFiles_logsErrorWhenLimitExceeded() {
        final Logger logger = (Logger) LogManager
                .getLogger(SegmentCompactionPolicy.class);
        final Level originalLevel = logger.getLevel();
        final TestAppender appender = new TestAppender();
        appender.start();
        logger.addAppender(appender);
        logger.setLevel(Level.ERROR);
        try {
            assertTrue(policy.shouldForceCompactionForDeltaFiles(
                    SegmentCompactionPolicy.MAX_DELTA_FILES_BEFORE_FORCE_COMPACTION
                            + 1));
            assertFalse(appender.events.isEmpty());
            final String message = appender.events.get(0).getMessage()
                    .getFormattedMessage();
            assertTrue(message.contains("Delta file limit exceeded"));
        } finally {
            logger.removeAppender(appender);
            appender.stop();
            logger.setLevel(originalLevel);
        }
    }

    private static final class TestAppender extends AbstractAppender {
        private final List<LogEvent> events = new CopyOnWriteArrayList<>();

        private TestAppender() {
            super("segmentCompactionPolicyTest", null,
                    PatternLayout.createDefaultLayout(), true, Property.EMPTY_ARRAY);
        }

        @Override
        public void append(final LogEvent event) {
            events.add(event.toImmutable());
        }
    }
}

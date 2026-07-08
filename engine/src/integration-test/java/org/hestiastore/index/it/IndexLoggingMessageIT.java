package org.hestiastore.index.it;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.StringWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.WriterAppender;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FileWriter;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.configuration.api.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.junit.jupiter.api.Test;

class IndexLoggingMessageIT {

    private static final String LOCK_FILE_NAME = ".lock";
    private static final String INDEX_NAME = "logging-message-it";
    private static final String LOG_PATTERN = "%d{ISO8601} %-5level [%t] "
            + "index='%X{index.name}' %msg%n%throwable";
    private static final String EXPECTED_INDEX_CONTEXT_FORMAT_FRAGMENT =
            " INFO  [main] index='" + INDEX_NAME + "' ";
    private static final String EXPECTED_EMPTY_INDEX_CONTEXT_FORMAT_FRAGMENT =
            " INFO  [main] index='' ";
    private static final String EXPECTED_RECOVERY_MESSAGE = "Recovered stale index lock (.lock). Index is going to be checked "
            + "for consistency and unlocked.";

    @Test
    void verify_bootstrap_log_includes_index_name_when_context_logging_enabled() {
        final Directory directory = new MemDirectory();
        final IndexConfiguration<Integer, String> configuration = IndexConfiguration.<Integer, String>builder()
                .identity(identity -> identity.keyClass(Integer.class)
                        .valueClass(String.class)
                        .name(INDEX_NAME))
                .logging(logging -> logging.contextEnabled(true))
                .build();
        final String logs = prepareLogMessages(directory, configuration);
        assertBootstrapRecoveryLog(logs,
                EXPECTED_INDEX_CONTEXT_FORMAT_FRAGMENT);
    }

    @Test
    void verify_bootstrap_log_includes_index_name_when_context_logging_uses_default() {
        final Directory directory = new MemDirectory();
        final IndexConfiguration<Integer, String> configuration = IndexConfiguration.<Integer, String>builder()
                .identity(identity -> identity.keyClass(Integer.class)
                        .valueClass(String.class)
                        .name(INDEX_NAME))
                .build();
        final String logs = prepareLogMessages(directory, configuration);
        assertBootstrapRecoveryLog(logs,
                EXPECTED_INDEX_CONTEXT_FORMAT_FRAGMENT);
    }

    @Test
    void verify_bootstrap_log_omits_index_name_when_context_logging_disabled() {
        final Directory directory = new MemDirectory();
        final IndexConfiguration<Integer, String> configuration = IndexConfiguration.<Integer, String>builder()
                .identity(identity -> identity.keyClass(Integer.class)
                        .valueClass(String.class)
                        .name(INDEX_NAME))
                .logging(logging -> logging.contextEnabled(false))
                .build();
        final String logs = prepareLogMessages(directory, configuration);
        assertBootstrapRecoveryLog(logs,
                EXPECTED_EMPTY_INDEX_CONTEXT_FORMAT_FRAGMENT);
    }

    private static void assertBootstrapRecoveryLog(final String logs,
            final String expectedFormatFragment) {
        assertAll(
                () -> assertTrue(
                        logs.contains(expectedFormatFragment),
                        () -> "Expected formatted INFO log fragment in:\n"
                                + logs),
                () -> assertTrue(logs.contains(EXPECTED_RECOVERY_MESSAGE),
                        () -> "Expected stale lock recovery message in:\n"
                                + logs));
    }

    String prepareLogMessages(final Directory directory, final IndexConfiguration<Integer, String> configuration) {
        createEmptyIndex(directory, configuration);
        writeStaleLock(directory);

        final StringWriter logWriter = new StringWriter();
        final WriterAppender appender = newWriterAppender(logWriter);
        final Logger rootLogger = (Logger) LogManager.getRootLogger();
        appender.start();
        rootLogger.addAppender(appender);
        String logs = null;
        try {
            try (SegmentIndex<Integer, String> index = SegmentIndex
                    .open(directory, configuration)) {
                index.put(1, "one");
                index.put(2, "two");
                index.put(3, "three");

                assertAll(
                        () -> assertEquals("one", index.get(1)),
                        () -> assertEquals("two", index.get(2)),
                        () -> assertEquals("three", index.get(3)));
            }

            logs = logWriter.toString();
        } finally {
            rootLogger.removeAppender(appender);
            appender.stop();
        }
        return logs;
    }

    private static void createEmptyIndex(final Directory directory,
            final IndexConfiguration<Integer, String> configuration) {
        try (SegmentIndex<Integer, String> index = SegmentIndex.create(
                directory, configuration)) {
            index.maintenance().flushAndWait();
        }
    }

    private static WriterAppender newWriterAppender(
            final StringWriter logWriter) {
        final PatternLayout layout = PatternLayout.newBuilder()
                .setPattern(LOG_PATTERN)
                .build();
        return WriterAppender.newBuilder()
                .setName("IndexLoggingMessageIT")
                .setLayout(layout)
                .setTarget(logWriter)
                .build();
    }

    private static void writeStaleLock(final Directory directory) {
        final String metadata = "version=1\n"
                + "pid=" + Long.MAX_VALUE + "\n"
                + "processStartEpochMillis=" + currentProcessStartMillis()
                + "\n"
                + "host=" + currentHost() + "\n"
                + "sessionId=stale-session\n";
        try (FileWriter writer = directory.getFileWriter(LOCK_FILE_NAME)) {
            writer.write(metadata.getBytes(StandardCharsets.UTF_8));
        }
    }

    private static String currentHost() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (final UnknownHostException e) {
            return "unknown-host";
        }
    }

    private static long currentProcessStartMillis() {
        return ProcessHandle.current().info().startInstant()
                .map(Instant::toEpochMilli)
                .orElse(-1L);
    }
}

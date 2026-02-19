package org.hestiastore.index.directory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;

import org.hestiastore.index.Vldtn;

/**
 * Metadata stored in lock files to distinguish live and stale locks.
 */
final class FileLockMetadata {

    private static final String FORMAT_VERSION = "1";
    private static final String KEY_VERSION = "version";
    private static final String KEY_PID = "pid";
    private static final String KEY_PROCESS_START_EPOCH_MILLIS = "processStartEpochMillis";
    private static final String KEY_HOST = "host";
    private static final String KEY_SESSION_ID = "sessionId";
    private static final String UNKNOWN_HOST = "unknown-host";
    private static final String CURRENT_HOST = resolveCurrentHost();
    private static final long CURRENT_PID = ProcessHandle.current().pid();
    private static final long CURRENT_PROCESS_START_EPOCH_MILLIS = resolveCurrentProcessStartEpochMillis();
    private static final String CURRENT_SESSION_ID = UUID.randomUUID()
            .toString();

    private final long pid;
    private final long processStartEpochMillis;
    private final String host;
    private final String sessionId;

    FileLockMetadata(final long pid, final long processStartEpochMillis,
            final String host, final String sessionId) {
        this.pid = pid;
        this.processStartEpochMillis = processStartEpochMillis;
        this.host = Vldtn.requireNonNull(host, "host");
        this.sessionId = Vldtn.requireNonNull(sessionId, "sessionId");
    }

    static FileLockMetadata currentProcess() {
        return new FileLockMetadata(CURRENT_PID,
                CURRENT_PROCESS_START_EPOCH_MILLIS, CURRENT_HOST,
                CURRENT_SESSION_ID);
    }

    static Optional<FileLockMetadata> read(final Directory directory,
            final String lockFileName) {
        Vldtn.requireNonNull(directory, "directory");
        Vldtn.requireNonNull(lockFileName, "lockFileName");
        if (!directory.isFileExists(lockFileName)) {
            return Optional.empty();
        }
        final byte[] bytes;
        try (FileReader reader = directory.getFileReader(lockFileName)) {
            bytes = readAllBytes(reader);
        } catch (final RuntimeException e) {
            return Optional.empty();
        }
        if (bytes.length == 0) {
            return Optional.empty();
        }
        final Properties properties = new Properties();
        try {
            properties.load(
                    new StringReader(new String(bytes, StandardCharsets.UTF_8)));
        } catch (final RuntimeException | IOException e) {
            return Optional.empty();
        }
        if (!FORMAT_VERSION.equals(properties.getProperty(KEY_VERSION))) {
            return Optional.empty();
        }
        final long pid = parseLong(properties.getProperty(KEY_PID), -1L);
        final long start = parseLong(
                properties.getProperty(KEY_PROCESS_START_EPOCH_MILLIS), -1L);
        final String host = properties.getProperty(KEY_HOST);
        final String sessionId = properties.getProperty(KEY_SESSION_ID);
        if (pid <= 0 || host == null || host.isBlank() || sessionId == null
                || sessionId.isBlank()) {
            return Optional.empty();
        }
        return Optional.of(new FileLockMetadata(pid, start, host, sessionId));
    }

    void write(final Directory directory, final String lockFileName) {
        Vldtn.requireNonNull(directory, "directory");
        Vldtn.requireNonNull(lockFileName, "lockFileName");
        final String metadata = KEY_VERSION + "=" + FORMAT_VERSION + "\n"
                + KEY_PID + "=" + pid + "\n"
                + KEY_PROCESS_START_EPOCH_MILLIS + "="
                + processStartEpochMillis + "\n"
                + KEY_HOST + "=" + host + "\n"
                + KEY_SESSION_ID + "=" + sessionId + "\n";
        try (FileWriter writer = directory.getFileWriter(lockFileName)) {
            writer.write(metadata.getBytes(StandardCharsets.UTF_8));
        }
    }

    boolean isOwnedByCurrentProcess() {
        if (!CURRENT_HOST.equals(host)) {
            return false;
        }
        if (CURRENT_PID != pid) {
            return false;
        }
        if (!CURRENT_SESSION_ID.equals(sessionId)) {
            return false;
        }
        return processStartEpochMillis <= 0
                || CURRENT_PROCESS_START_EPOCH_MILLIS <= 0
                || processStartEpochMillis == CURRENT_PROCESS_START_EPOCH_MILLIS;
    }

    boolean canRecoverAsStale() {
        if (!CURRENT_HOST.equals(host)) {
            return false;
        }
        if (isOwnedByCurrentProcess()) {
            return false;
        }
        final Optional<ProcessHandle> oHandle = ProcessHandle.of(pid);
        if (oHandle.isEmpty()) {
            return true;
        }
        final ProcessHandle handle = oHandle.get();
        if (!handle.isAlive()) {
            return true;
        }
        if (processStartEpochMillis > 0) {
            final Optional<Instant> oStart = handle.info().startInstant();
            if (oStart.isPresent()) {
                return oStart.get().toEpochMilli() != processStartEpochMillis;
            }
        }
        return false;
    }

    boolean hasSameSession(final FileLockMetadata other) {
        return other != null && sessionId.equals(other.sessionId);
    }

    long getPid() {
        return pid;
    }

    long getProcessStartEpochMillis() {
        return processStartEpochMillis;
    }

    String getHost() {
        return host;
    }

    private static byte[] readAllBytes(final FileReader reader) {
        final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        final byte[] chunk = new byte[256];
        while (true) {
            final int read = reader.read(chunk);
            if (read < 0) {
                break;
            }
            buffer.write(chunk, 0, read);
        }
        return buffer.toByteArray();
    }

    private static long parseLong(final String value,
            final long defaultValue) {
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value);
        } catch (final NumberFormatException e) {
            return defaultValue;
        }
    }

    private static String resolveCurrentHost() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (final UnknownHostException e) {
            return UNKNOWN_HOST;
        }
    }

    private static long resolveCurrentProcessStartEpochMillis() {
        return ProcessHandle.current().info().startInstant()
                .map(Instant::toEpochMilli).orElse(-1L);
    }
}

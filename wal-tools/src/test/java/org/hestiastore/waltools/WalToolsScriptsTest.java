package org.hestiastore.waltools;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

class WalToolsScriptsTest {

    private static final String FUNCTIONALITY_HEADER = "# Functionality:";
    private static final String USAGE_VERIFY = "Usage: wal_verify <walDirectoryPath> [--json]";
    private static final String USAGE_DUMP = "Usage: wal_dump <walDirectoryPath> [--json]";
    private static final String USAGE_TOOL = "Usage: wal_tool.sh <verify|dump> <walDirectoryPath> [--json]";

    @Test
    void scriptsContainFunctionalitySections() throws IOException {
        assertScriptContains("wal_tool.sh", FUNCTIONALITY_HEADER,
                "Shared launcher for WAL tooling commands");
        assertScriptContains("wal_verify", FUNCTIONALITY_HEADER,
                "Operator shortcut for WAL verification");
        assertScriptContains("wal_dump", FUNCTIONALITY_HEADER,
                "Operator shortcut for WAL dump/inspection output");
    }

    @Test
    void wrappersPrintUsageWhenWalDirectoryIsMissing() throws IOException {
        assumeUnixLikeEnvironment();

        final String verifyOutput = runScriptExpectingExit(script("wal_verify"),
                1);
        assertTrue(verifyOutput.contains(USAGE_VERIFY));

        final String dumpOutput = runScriptExpectingExit(script("wal_dump"), 1);
        assertTrue(dumpOutput.contains(USAGE_DUMP));
    }

    @Test
    void walToolScriptPrintsUsageForInvalidCommand() throws IOException {
        assumeUnixLikeEnvironment();

        final String output = runScriptExpectingExit(script("wal_tool.sh"), 1,
                "invalid", "/tmp/wal");
        assertTrue(output.contains(USAGE_TOOL));
    }

    @Test
    void walToolScriptReportsMissingDistributionLibraries() throws IOException {
        assumeUnixLikeEnvironment();

        final String output = runScriptExpectingExit(script("wal_tool.sh"), 1,
                "verify", "/tmp/wal");
        assertTrue(output.contains("Missing distribution lib directory"));
    }

    private static void assertScriptContains(final String scriptName,
            final String... expectedSnippets) throws IOException {
        final Path script = script(scriptName);
        final String content = Files.readString(script);
        for (String snippet : expectedSnippets) {
            assertTrue(content.contains(snippet));
        }
    }

    private static Path script(final String name) {
        return Path.of("src/main/bin", name).toAbsolutePath().normalize();
    }

    private static String runScriptExpectingExit(final Path script,
            final int expectedExitCode, final String... args) throws IOException {
        final List<String> command = new ArrayList<>();
        command.add(script.toString());
        command.addAll(Arrays.asList(args));

        final ProcessBuilder builder = new ProcessBuilder(command);
        builder.redirectErrorStream(true);
        final Process process = builder.start();

        final String output;
        try (InputStream stream = process.getInputStream()) {
            output = new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        }

        final int exitCode = waitFor(process);
        assertEquals(expectedExitCode, exitCode);
        return output;
    }

    private static int waitFor(final Process process) {
        try {
            return process.waitFor();
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
            throw new AssertionError(
                    "Interrupted while waiting for script execution.",
                    interruptedException);
        }
    }

    private static void assumeUnixLikeEnvironment() {
        final String osName = System.getProperty("os.name", "");
        final String osNameLower = osName.toLowerCase(Locale.ROOT);
        Assumptions.assumeFalse(osNameLower.contains("win"),
                "Shell script tests require a Unix-like environment.");
    }
}

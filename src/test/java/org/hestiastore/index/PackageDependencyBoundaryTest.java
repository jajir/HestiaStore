package org.hestiastore.index;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

/**
 * Enforces source-level package dependency boundaries.
 */
class PackageDependencyBoundaryTest {

    private static final Path MAIN_JAVA = Path.of("src", "main", "java");
    private static final String CORE_ROOT = "org/hestiastore/index/";
    private static final String MONITORING_ROOT = "org/hestiastore/index/monitoring/";
    private static final String MANAGEMENT_ROOT = "org/hestiastore/index/management/";
    private static final String CONSOLE_ROOT = "org/hestiastore/console/";

    private static final String[] FORBIDDEN_IMPORT_PREFIXES = {
            "org.hestiastore.index.monitoring.",
            "org.hestiastore.index.management.",
            "org.hestiastore.console." };

    @Test
    void corePackages_doNotImportMonitoringManagementOrConsole()
            throws IOException {
        final List<String> violations = new ArrayList<>();
        try (Stream<Path> paths = Files.walk(MAIN_JAVA)) {
            paths.filter(Files::isRegularFile).filter(this::isJavaFile)
                    .filter(this::isCoreSource).forEach(path -> {
                        collectViolations(path, violations);
                    });
        }
        assertTrue(violations.isEmpty(),
                () -> "Found forbidden imports:\n" + String.join("\n",
                        violations));
    }

    private boolean isJavaFile(final Path path) {
        return path.toString().endsWith(".java");
    }

    private boolean isCoreSource(final Path path) {
        final String normalized = path.toString().replace('\\', '/');
        if (!normalized.contains(CORE_ROOT)) {
            return false;
        }
        return !normalized.contains(MONITORING_ROOT)
                && !normalized.contains(MANAGEMENT_ROOT)
                && !normalized.contains(CONSOLE_ROOT);
    }

    private void collectViolations(final Path path,
            final List<String> violations) {
        final List<String> lines;
        try {
            lines = Files.readAllLines(path);
        } catch (final IOException e) {
            throw new RuntimeException(
                    "Failed to read source file " + path, e);
        }
        for (int i = 0; i < lines.size(); i++) {
            final String trimmed = lines.get(i).trim();
            if (!trimmed.startsWith("import ")) {
                continue;
            }
            for (String forbiddenPrefix : FORBIDDEN_IMPORT_PREFIXES) {
                if (trimmed.startsWith("import " + forbiddenPrefix)) {
                    violations.add(path + ":" + (i + 1) + " -> " + trimmed);
                }
            }
        }
    }
}

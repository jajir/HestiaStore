package com.coroptis.index.loadtest;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.logging.Logger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class SimpleIT {

    private final Logger logger = Logger.getLogger(SimpleIT.class.getName());

    @Test
    void test_simple2() throws Exception {
        final Path jarFile = Paths.get("target/load-test2.jar");
        logger.info(jarFile.toFile().getAbsolutePath());
        final ProcessBuilder builder = new ProcessBuilder(//
                "java", //
                "-Xmx10000m", //
                "-cp", //
                jarFile.toFile().getAbsolutePath(), //
                "com.coroptis.index.loadtest.Main", //
                "--test1", //
                "")//
                .inheritIO();
        final Process process = builder.start();

        logger.info("Waiting for process finishing preparing data");
        await().atMost(30, SECONDS).pollInterval(1, SECONDS)
                .until(TestStatus::isReadyToTest);
        assertTrue(process.isAlive(), "Process should be terminated");
        logger.info("Now it's ready to terminate");

        Thread.sleep(1000);
        process.destroy(); // graceful SIGTERM
        assertFalse(process.isAlive(), "Process should be terminated");
    }

    @Test
    void test_simple3() throws Exception {
    }

    @BeforeAll
    void setUp() {
        TestStatus.reset();
        deleteDirectoryRecursively(new File(ConsistencyCheck.DIRECTORY));

    }

    @AfterEach
    void tearDown() {
        TestStatus.reset();
    }

    public static boolean deleteDirectoryRecursively(File directory) {
        if (directory == null || !directory.exists()) {
            return true; // Nothing to delete
        }

        final File[] files = directory.listFiles();
        if (files != null) { // Not a file, and not null
            for (final File file : files) {
                boolean success = file.isDirectory()
                        ? deleteDirectoryRecursively(file)
                        : file.delete();
                if (!success) {
                    return false;
                }
            }
        }
        return directory.delete(); // Delete the empty directory itself
    }

}

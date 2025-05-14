package com.coroptis.index.loadtest;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class TestStatus {

    private final static Path IS_READY_FILE = Paths
            .get("target/is-ready.signal");

    public static boolean isReadyToTest() {
        return Files.exists(IS_READY_FILE);
    }

    public static void reset() {
        try {
            Files.deleteIfExists(IS_READY_FILE);
        } catch (IOException e) {
            throw new RuntimeException("Error reseting test status", e);
        }
    }

    static void setReadyToTest(boolean isReady) {
        try {
            if (isReady) {
                Files.createFile(IS_READY_FILE);
            } else {
                Files.deleteIfExists(IS_READY_FILE);
            }
        } catch (IOException e) {
            throw new RuntimeException("Error setting test status", e);
        }
    }
}

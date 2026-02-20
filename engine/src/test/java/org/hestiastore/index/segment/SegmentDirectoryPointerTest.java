package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.Test;

class SegmentDirectoryPointerTest {

    @Test
    void readActiveDirectory_returns_null_when_missing() {
        final Directory directory = new MemDirectory();
        final SegmentDirectoryPointer pointer = new SegmentDirectoryPointer(
                directory, new SegmentDirectoryLayout(SegmentId.of(1)));

        assertNull(pointer.readActiveDirectory());
    }

    @Test
    void writeActiveDirectory_persists_value() {
        final Directory directory = new MemDirectory();
        final SegmentDirectoryPointer pointer = new SegmentDirectoryPointer(
                directory, new SegmentDirectoryLayout(SegmentId.of(1)));

        pointer.writeActiveDirectory("v1");

        assertEquals("v1", pointer.readActiveDirectory());
    }

    @Test
    void writeActiveDirectory_rejects_blank_value() {
        final Directory directory = new MemDirectory();
        final SegmentDirectoryPointer pointer = new SegmentDirectoryPointer(
                directory, new SegmentDirectoryLayout(SegmentId.of(1)));

        assertThrows(IllegalArgumentException.class,
                () -> pointer.writeActiveDirectory(" "));
    }
}

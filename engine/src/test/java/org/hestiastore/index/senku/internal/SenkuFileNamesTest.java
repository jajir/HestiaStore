package org.hestiastore.index.senku.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hestiastore.index.IndexException;
import org.junit.jupiter.api.Test;

class SenkuFileNamesTest {

    @Test
    void names_useMinimumWidthAndGrowNaturally() {
        assertEquals("flush-00000", SenkuFileNames.flushDirectory(0));
        assertEquals("flush-00001", SenkuFileNames.flushDirectory(1));
        assertEquals("flush-100000", SenkuFileNames.flushDirectory(100_000));
        assertEquals("shard-00007", SenkuFileNames.shardDirectory(7));
        assertEquals("level-00012", SenkuFileNames.levelDirectory(12));
        assertEquals("run-00042", SenkuFileNames.runDirectory(42));
        assertEquals("part-00003.chunk", SenkuFileNames.partFile(3));
    }

    @Test
    void parsers_acceptCanonicalNames() {
        assertEquals(100_000L,
                SenkuFileNames.parseFlushDirectory("flush-100000"));
        assertEquals(7, SenkuFileNames.parseShardDirectory("shard-00007"));
        assertEquals(12, SenkuFileNames.parseLevelDirectory("level-00012"));
        assertEquals(Long.MAX_VALUE, SenkuFileNames.parseRunDirectory(
                "run-9223372036854775807"));
        assertEquals(3, SenkuFileNames.parsePartFile("part-00003.chunk"));
    }

    @Test
    void parsers_rejectAliasesAndMalformedNames() {
        assertInvalid(() -> SenkuFileNames.parseRunDirectory("run-1"));
        assertInvalid(() -> SenkuFileNames.parseRunDirectory("run-000001"));
        assertInvalid(() -> SenkuFileNames.parseRunDirectory("run-+0001"));
        assertInvalid(() -> SenkuFileNames.parseRunDirectory("run--0001"));
        assertInvalid(() -> SenkuFileNames.parseRunDirectory("run- 0001"));
        assertInvalid(() -> SenkuFileNames.parseRunDirectory("run-abcde"));
        assertInvalid(() -> SenkuFileNames.parsePartFile("part-00001"));
        assertInvalid(() -> SenkuFileNames.parsePartFile("run-00001.chunk"));
    }

    @Test
    void parsers_rejectNumericOverflow() {
        assertInvalid(() -> SenkuFileNames.parseRunDirectory(
                "run-9223372036854775808"));
        assertInvalid(() -> SenkuFileNames.parseShardDirectory(
                "shard-2147483648"));
        assertInvalid(() -> SenkuFileNames.parsePartFile(
                "part-2147483647.chunk"));
    }

    @Test
    void formatters_rejectNegativeAndUnsupportedPartNumbers() {
        assertThrows(IllegalArgumentException.class,
                () -> SenkuFileNames.flushDirectory(-1));
        assertThrows(IllegalArgumentException.class,
                () -> SenkuFileNames.shardDirectory(-1));
        assertThrows(IllegalArgumentException.class,
                () -> SenkuFileNames.partFile(Integer.MAX_VALUE));
    }

    @Test
    void temporary_appendsSuffixToCanonicalName() {
        assertEquals("manifest.properties.tmp",
                SenkuFileNames.temporary(SenkuFileNames.MANIFEST_FILE));
    }

    private static void assertInvalid(final Runnable operation) {
        assertThrows(IndexException.class, operation::run);
    }
}

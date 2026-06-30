package org.hestiastore.index.segmentindex.config;

/**
 * Compatibility shim for a historically misspelled test class name.
 *
 * <p>The correctly spelled coverage lives in
 * {@link IndexConfigurationStorageWalTest}. This empty abstract type ensures a
 * plain {@code mvn test} overwrites stale compiled classes left behind by the
 * old typo-named source file, even when the build runs without {@code clean}.
 */
abstract class IndexConfiguratonStorageWalTest {
}

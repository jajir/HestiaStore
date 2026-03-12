package org.hestiastore.index.segmentindex.config;

/**
 * Compatibility shim for a historically misspelled test class name.
 *
 * <p>The correctly spelled coverage lives in
 * {@link SegmentIndexConfigurationStorageTest}. This empty abstract type
 * overwrites stale typo-named class files during test compilation so Surefire
 * does not execute removed bytecode from previous non-clean builds.
 */
abstract class SegmentIndexConfiguratonStorageTest {
}

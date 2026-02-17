/**
 * Sorted-data-file primitives for building and reading sorted key/value files.
 *
 * <p>
 * This package contains writers, readers, iterators, and sort/merge utilities
 * used to construct SST-style files. Writers expect keys in ascending order
 * and readers stream entries back in sorted order. Helper classes provide
 * diff-key encoding and multi-way merge logic.
 * </p>
 */
package org.hestiastore.index.sorteddatafile;

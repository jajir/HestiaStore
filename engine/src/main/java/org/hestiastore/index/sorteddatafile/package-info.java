/**
 * Sorted-data-file primitives for building and reading sorted key/value files.
 *
 * <p>
 * This package contains writers, readers, and iterators used to construct
 * SST-style files. Writers expect keys in ascending order and readers stream
 * entries back in sorted order. Sorting and merge helpers live in the
 * {@code org.hestiastore.index.sorteddatafile.sort} package.
 * </p>
 */
package org.hestiastore.index.sorteddatafile;

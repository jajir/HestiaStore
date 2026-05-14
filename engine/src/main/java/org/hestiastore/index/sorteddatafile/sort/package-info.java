/**
 * Sorting and merge helpers for constructing sorted data files.
 *
 * <p>
 * This package owns external-sort orchestration and duplicate-key merge
 * utilities. It depends on sorted-data-file primitives for input and output,
 * while keeping those primitives independent from cache-backed sorting.
 * </p>
 */
package org.hestiastore.index.sorteddatafile.sort;

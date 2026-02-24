/**
 * Public API for segment index operations.
 *
 * Implementation details are split into focused subpackages:
 * <ul>
 * <li>{@code config} for configuration loading/defaulting/storage</li>
 * <li>{@code core} for lifecycle and execution flow</li>
 * <li>{@code mapping} for key-to-segment mapping logic</li>
 * <li>{@code split} for split planning/coordinator/pipeline internals</li>
 * </ul>
 *
 * @author jajir
 */
package org.hestiastore.index.segmentindex;

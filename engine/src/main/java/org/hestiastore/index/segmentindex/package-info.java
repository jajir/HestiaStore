/**
 * Public API for segment index operations.
 *
 * Implementation details are split into focused subpackages:
 * <ul>
 * <li>{@code configuration.defaults} for open-time default providers</li>
 * <li>{@code configuration.persistence} for loading, merging, validating, and
 * storing persisted index configuration</li>
* <li>{@code configuration.types} for Java type to descriptor defaults</li>
* <li>{@code tuning} for live runtime tuning APIs and implementation</li>
* <li>{@code core} for lifecycle and execution flow</li>
 * <li>{@code mapping} for key-to-segment mapping logic</li>
 * <li>{@code split} for split planning/coordinator/pipeline internals</li>
 * </ul>
 *
 * @author jajir
 */
package org.hestiastore.index.segmentindex;

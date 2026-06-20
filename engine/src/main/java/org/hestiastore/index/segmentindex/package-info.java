/**
 * Public API for segment index operations.
 *
 * Implementation details are split into focused subpackages:
 * <ul>
 * <li>{@code configuration.*} for user, effective, persisted, default, and
 * runtime-tunable configuration</li>
 * <li>{@code core.execution} for foreground, maintenance, streaming, and
 * stable-segment operation flow</li>
 * <li>{@code core.routing} for route state and scoped segment leases</li>
 * <li>{@code core.split} for split planning/coordinator/pipeline internals</li>
 * <li>{@code core.storage} for durable state, WAL coordination, consistency,
 * and cleanup</li>
 * <li>{@code routemap} for key-to-segment route-map persistence and snapshots</li>
 * <li>{@code wal} for WAL format, catalog, runtime, and tooling</li>
 * </ul>
 *
 * @author jajir
 */
package org.hestiastore.index.segmentindex;

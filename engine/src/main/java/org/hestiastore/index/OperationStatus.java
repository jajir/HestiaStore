package org.hestiastore.index;

/**
 * Public operation status used by legacy segment and registry APIs.
 * <p>
 * {@link #WRITE_CACHE_FULL} means the raw segment could not accept the
 * operation because write-cache capacity is currently exhausted. Registry
 * blocking calls interpret it according to automatic-maintenance configuration:
 * retryable when maintenance is enabled, terminal when maintenance is disabled.
 */
public enum OperationStatus {
    OK,
    BUSY,
    WRITE_CACHE_FULL,
    CLOSED,
    ERROR
}

package org.hestiastore.index;

/**
 * Public operation status used by legacy segment and registry APIs.
 */
public enum OperationStatus {
    OK,
    BUSY,
    CLOSED,
    ERROR
}

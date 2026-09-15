package org.hestiastore.index.senku.internal;

/**
 * Internal writing-handle lifecycle without a public preparing state.
 */
enum SenkuWritingState {
    WRITING,
    FINISHING,
    TRANSFERRED,
    ERROR
}

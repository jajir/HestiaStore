package org.hestiastore.index.segmentindex.core.storage;

import static org.mockito.Mockito.inOrder;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class WalRetentionPressureCheckpointTest {

    @Mock
    private WalCheckpointDurableState durableState;

    @Mock
    private WalCheckpointExecutor<Integer, String> checkpointExecutor;

    private WalRetentionPressureCheckpoint<Integer, String> checkpoint;

    @BeforeEach
    void setUp() {
        checkpoint = new WalRetentionPressureCheckpoint<>(durableState,
                checkpointExecutor);
    }

    @Test
    void forceCheckpointFlushesDurableStateBeforeWalCheckpoint() {
        checkpoint.forceCheckpoint();

        final InOrder inOrder = inOrder(durableState, checkpointExecutor);
        inOrder.verify(durableState).flushBeforeWalCheckpoint();
        inOrder.verify(checkpointExecutor).checkpointInternal();
    }
}

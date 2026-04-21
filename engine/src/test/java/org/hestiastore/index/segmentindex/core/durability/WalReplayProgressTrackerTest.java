package org.hestiastore.index.segmentindex.core.durability;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.AtomicLong;

import org.hestiastore.index.segmentindex.wal.WalRuntime;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class WalReplayProgressTrackerTest {

    @Mock
    private WalRuntime<Integer, String> walRuntime;

    @Test
    void recover_updatesLastAppliedLsnFromRecoveryMax() {
        final AtomicLong lastAppliedWalLsn = new AtomicLong(5L);
        final WalReplayProgressTracker<Integer, String> tracker =
                new WalReplayProgressTracker<>(walRuntime, lastAppliedWalLsn);
        when(walRuntime.isEnabled()).thenReturn(true);
        when(walRuntime.recover(any()))
                .thenReturn(new WalRuntime.RecoveryResult(3L, 7L, false));

        tracker.recover(replayRecord -> {
        });

        assertEquals(7L, lastAppliedWalLsn.get());
        verify(walRuntime).recover(any());
    }

    @Test
    void recordAppliedLsn_keepsMaximumObservedValue() {
        final AtomicLong lastAppliedWalLsn = new AtomicLong(0L);
        final WalReplayProgressTracker<Integer, String> tracker =
                new WalReplayProgressTracker<>(walRuntime, lastAppliedWalLsn);
        when(walRuntime.isEnabled()).thenReturn(true);

        tracker.recordAppliedLsn(4L);
        tracker.recordAppliedLsn(2L);
        tracker.recordAppliedLsn(9L);

        assertEquals(9L, lastAppliedWalLsn.get());
    }
}

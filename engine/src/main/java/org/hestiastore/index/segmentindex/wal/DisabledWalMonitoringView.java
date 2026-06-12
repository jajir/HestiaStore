package org.hestiastore.index.segmentindex.wal;

final class DisabledWalMonitoringView implements WalMonitoringView {

    static final DisabledWalMonitoringView INSTANCE =
            new DisabledWalMonitoringView();

    private DisabledWalMonitoringView() {
    }

    @Override
    public WalMonitoring statsSnapshot() {
        return WalMonitoring.empty();
    }
}

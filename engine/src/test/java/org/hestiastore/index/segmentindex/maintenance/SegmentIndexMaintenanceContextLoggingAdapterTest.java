package org.hestiastore.index.segmentindex.maintenance;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.util.concurrent.atomic.AtomicReference;

import org.hestiastore.index.segmentindex.core.IndexMdcScopeRunner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.MDC;

@ExtendWith(MockitoExtension.class)
class SegmentIndexMaintenanceContextLoggingAdapterTest {

    @AfterEach
    void tearDown() {
        MDC.clear();
    }

    @Test
    void maintenanceMethodsRunWithIndexContext() {
        final SegmentIndexMaintenance delegate = mock(
                SegmentIndexMaintenance.class);
        final AtomicReference<String> compactMdc = new AtomicReference<>();
        final AtomicReference<String> compactAndWaitMdc =
                new AtomicReference<>();
        final AtomicReference<String> flushMdc = new AtomicReference<>();
        final AtomicReference<String> flushAndWaitMdc = new AtomicReference<>();
        final AtomicReference<String> repairMdc = new AtomicReference<>();
        doAnswer(invocation -> {
            compactMdc.set(MDC.get("index.name"));
            return null;
        }).when(delegate).compact();
        doAnswer(invocation -> {
            compactAndWaitMdc.set(MDC.get("index.name"));
            return null;
        }).when(delegate).compactAndWait();
        doAnswer(invocation -> {
            flushMdc.set(MDC.get("index.name"));
            return null;
        }).when(delegate).flush();
        doAnswer(invocation -> {
            flushAndWaitMdc.set(MDC.get("index.name"));
            return null;
        }).when(delegate).flushAndWait();
        doAnswer(invocation -> {
            repairMdc.set(MDC.get("index.name"));
            return null;
        }).when(delegate).checkAndRepairConsistency();

        final SegmentIndexMaintenanceContextLoggingAdapter adapter =
                new SegmentIndexMaintenanceContextLoggingAdapter(delegate,
                        new IndexMdcScopeRunner("idx"));
        adapter.compact();
        adapter.compactAndWait();
        adapter.flush();
        adapter.flushAndWait();
        adapter.checkAndRepairConsistency();

        assertEquals("idx", compactMdc.get());
        assertEquals("idx", compactAndWaitMdc.get());
        assertEquals("idx", flushMdc.get());
        assertEquals("idx", flushAndWaitMdc.get());
        assertEquals("idx", repairMdc.get());
    }
}

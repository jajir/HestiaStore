package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.reflect.Field;
import java.util.List;

import org.hestiastore.index.Entry;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.directory.async.AsyncDirectory;
import org.hestiastore.index.directory.async.AsyncDirectoryAdapter;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.sorteddatafile.SortedDataFile;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentSplitCoordinatorConcurrencyTest {

    private static final TypeDescriptorInteger KEY_DESCRIPTOR = new TypeDescriptorInteger();
    private static final TypeDescriptorShortString VALUE_DESCRIPTOR = new TypeDescriptorShortString();
    @Mock
    private IndexConfiguration<Integer, String> registryConf;

    private SegmentRegistryImpl<Integer, String> registry;
    private KeyToSegmentMapSynchronizedAdapter<Integer> keyToSegmentMap;
    private SegmentSplitCoordinator<Integer, String> coordinator;

    @BeforeEach
    void setUp() {
        Mockito.when(registryConf.getNumberOfSegmentIndexMaintenanceThreads())
                .thenReturn(1);
        Mockito.when(registryConf.getNumberOfIndexMaintenanceThreads())
                .thenReturn(1);
        Mockito.when(registryConf.getMaxNumberOfSegmentsInCache()).thenReturn(3);
        final AsyncDirectory directoryFacade = AsyncDirectoryAdapter
                .wrap(new MemDirectory());
        registry = new SegmentRegistryImpl<>(directoryFacade, KEY_DESCRIPTOR,
                VALUE_DESCRIPTOR, registryConf);
        final KeyToSegmentMap<Integer> rawKeyMap = newCacheWithEntries(List.of(
                Entry.of(10, SegmentId.of(1)),
                Entry.of(30, SegmentId.of(4))));
        keyToSegmentMap = new KeyToSegmentMapSynchronizedAdapter<>(rawKeyMap);
        final IndexConfiguration<Integer, String> coordinatorConf = IndexConfiguration
                .<Integer, String>builder()
                .withIndexBusyBackoffMillis(1)
                .withIndexBusyTimeoutMillis(50)
                .build();
        coordinator = new SegmentSplitCoordinator<>(coordinatorConf,
                keyToSegmentMap, registry);
    }

    @AfterEach
    void tearDown() {
        if (keyToSegmentMap != null && !keyToSegmentMap.wasClosed()) {
            keyToSegmentMap.close();
        }
        if (registry != null) {
            registry.close();
        }
        coordinator = null;
        keyToSegmentMap = null;
        registry = null;
    }

    @Test
    void applySplitPlan_returns_false_when_registry_frozen() {
        final SegmentSplitApplyPlan<Integer, String> plan = new SegmentSplitApplyPlan<>(
                SegmentId.of(1), SegmentId.of(2), SegmentId.of(3), 1, 5,
                SegmentSplitterResult.SegmentSplittingStatus.SPLIT);
        final SegmentRegistryGate gate = readGate(registry);

        assertTrue(gate.tryEnterFreeze());
        try (GateGuard ignored = new GateGuard(gate)) {
            assertFalse(coordinator.applySplitPlan(plan).isOk());
        }

        assertEquals(List.of(SegmentId.of(1), SegmentId.of(4)),
                keyToSegmentMap.getSegmentIds());
    }

    @Test
    void applySplitPlan_retries_after_registry_freeze() {
        final SegmentSplitApplyPlan<Integer, String> plan = new SegmentSplitApplyPlan<>(
                SegmentId.of(1), SegmentId.of(2), SegmentId.of(3), 1, 5,
                SegmentSplitterResult.SegmentSplittingStatus.SPLIT);
        final SegmentRegistryGate gate = readGate(registry);

        assertTrue(gate.tryEnterFreeze());
        try (GateGuard ignored = new GateGuard(gate)) {
            assertFalse(coordinator.applySplitPlan(plan).isOk());
        }

        assertTrue(coordinator.applySplitPlan(plan).isOk());
        assertEquals(List.of(SegmentId.of(2), SegmentId.of(3), SegmentId.of(4)),
                keyToSegmentMap.getSegmentIds());
    }

    @Test
    void applySplitPlan_requires_registry_lock_before_key_map_when_enforced() {
        final SegmentSplitApplyPlan<Integer, String> plan = new SegmentSplitApplyPlan<>(
                SegmentId.of(1), SegmentId.of(2), SegmentId.of(3), 1, 5,
                SegmentSplitterResult.SegmentSplittingStatus.SPLIT);
        final String previousEnforce = System.getProperty(
                "hestiastore.enforceSplitLockOrder");
        final String previousRegistry = System.getProperty(
                "hestiastore.registryLockHeld");
        final String previousKeyMap = System.getProperty(
                "hestiastore.keyMapLockHeld");
        System.setProperty("hestiastore.enforceSplitLockOrder", "true");
        System.clearProperty("hestiastore.registryLockHeld");
        System.setProperty("hestiastore.keyMapLockHeld", "true");
        try {
            assertThrows(IllegalStateException.class,
                    () -> registry.applySplitPlan(plan, null, null, () -> true));
        } finally {
            if (previousEnforce == null) {
                System.clearProperty("hestiastore.enforceSplitLockOrder");
            } else {
                System.setProperty("hestiastore.enforceSplitLockOrder",
                        previousEnforce);
            }
            if (previousRegistry == null) {
                System.clearProperty("hestiastore.registryLockHeld");
            } else {
                System.setProperty("hestiastore.registryLockHeld",
                        previousRegistry);
            }
            if (previousKeyMap == null) {
                System.clearProperty("hestiastore.keyMapLockHeld");
            } else {
                System.setProperty("hestiastore.keyMapLockHeld",
                        previousKeyMap);
            }
        }
    }

    private KeyToSegmentMap<Integer> newCacheWithEntries(
            final List<Entry<Integer, SegmentId>> entries) {
        final MemDirectory dir = new MemDirectory();
        final SortedDataFile<Integer, SegmentId> sdf = SortedDataFile
                .<Integer, SegmentId>builder()
                .withAsyncDirectory(AsyncDirectoryAdapter.wrap(dir))
                .withFileName("index.map")
                .withKeyTypeDescriptor(new TypeDescriptorInteger())
                .withValueTypeDescriptor(new TypeDescriptorSegmentId())
                .build();
        sdf.openWriterTx()
                .execute(writer -> entries.stream().sorted(
                        (e1, e2) -> Integer.compare(e1.getKey(), e2.getKey()))
                        .forEach(writer::write));
        return new KeyToSegmentMap<>(AsyncDirectoryAdapter.wrap(dir),
                new TypeDescriptorInteger());
    }

    private SegmentRegistryGate readGate(
            final SegmentRegistryImpl<Integer, String> target) {
        try {
            final Field field = SegmentRegistryImpl.class
                    .getDeclaredField("gate");
            field.setAccessible(true);
            return (SegmentRegistryGate) field.get(target);
        } catch (final ReflectiveOperationException ex) {
            throw new IllegalStateException(
                    "Unable to read gate for test", ex);
        }
    }

    private static final class GateGuard implements AutoCloseable {
        private final SegmentRegistryGate gate;

        private GateGuard(final SegmentRegistryGate gate) {
            this.gate = gate;
        }

        @Override
        public void close() {
            gate.finishFreezeToReady();
        }
    }
}

package org.hestiastore.index.segmentindex.core;

import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.control.IndexConfigurationManagement;
import org.hestiastore.index.control.IndexControlPlane;
import org.hestiastore.index.control.IndexRuntimeView;
import org.hestiastore.index.control.model.ConfigurationSnapshot;
import org.hestiastore.index.control.model.IndexRuntimeSnapshot;
import org.hestiastore.index.control.model.RuntimeConfigPatch;
import org.hestiastore.index.control.model.RuntimePatchResult;
import org.hestiastore.index.control.model.RuntimePatchValidation;
import org.hestiastore.index.control.model.RuntimeSettingKey;
import org.hestiastore.index.control.model.ValidationIssue;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.SegmentIndexState;

/**
 * Implements runtime monitoring and runtime patching over one index instance.
 */
final class IndexRuntimeControlPlane implements IndexControlPlane {

    private final IndexConfiguration<?, ?> conf;
    private final RuntimeTuningState runtimeTuningState;
    private final Supplier<SegmentIndexState> stateSupplier;
    private final Supplier<SegmentIndexMetricsSnapshot> metricsSnapshotSupplier;
    private final Consumer<Map<RuntimeSettingKey, Integer>> effectiveLimitsApplier;
    private final Runnable splitThresholdChangedListener;
    private final IndexRuntimeView runtime = new IndexRuntimeViewImpl();
    private final IndexConfigurationManagement configuration =
            new IndexConfigurationManagementImpl();

    IndexRuntimeControlPlane(final IndexConfiguration<?, ?> conf,
            final RuntimeTuningState runtimeTuningState,
            final Supplier<SegmentIndexState> stateSupplier,
            final Supplier<SegmentIndexMetricsSnapshot> metricsSnapshotSupplier,
            final Consumer<Map<RuntimeSettingKey, Integer>> effectiveLimitsApplier,
            final Runnable splitThresholdChangedListener) {
        this.conf = Vldtn.requireNonNull(conf, "conf");
        this.runtimeTuningState = Vldtn.requireNonNull(runtimeTuningState,
                "runtimeTuningState");
        this.stateSupplier = Vldtn.requireNonNull(stateSupplier,
                "stateSupplier");
        this.metricsSnapshotSupplier = Vldtn.requireNonNull(
                metricsSnapshotSupplier, "metricsSnapshotSupplier");
        this.effectiveLimitsApplier = Vldtn.requireNonNull(
                effectiveLimitsApplier, "effectiveLimitsApplier");
        this.splitThresholdChangedListener = Vldtn.requireNonNull(
                splitThresholdChangedListener, "splitThresholdChangedListener");
    }

    @Override
    public String indexName() {
        return conf.getIndexName();
    }

    @Override
    public IndexRuntimeView runtime() {
        return runtime;
    }

    @Override
    public IndexConfigurationManagement configuration() {
        return configuration;
    }

    RuntimePatchValidation validate(final RuntimeConfigPatch patch) {
        final List<ValidationIssue> issues = new ArrayList<>();
        final EnumMap<RuntimeSettingKey, Integer> normalized = new EnumMap<>(
                RuntimeSettingKey.class);
        if (patch == null) {
            issues.add(new ValidationIssue(null, "patch must not be null"));
            return new RuntimePatchValidation(false, issues, normalized);
        }
        if (patch.expectedRevision() != null && patch.expectedRevision()
                .longValue() != runtimeTuningState.revision()) {
            issues.add(new ValidationIssue(null,
                    "expectedRevision does not match current revision"));
        }
        for (final Map.Entry<RuntimeSettingKey, Integer> entry : patch.values()
                .entrySet()) {
            final RuntimeSettingKey key = entry.getKey();
            final int value = entry.getValue().intValue();
            if (value < 1) {
                issues.add(new ValidationIssue(key, "value must be >= 1"));
            } else if (key == RuntimeSettingKey.MAX_NUMBER_OF_SEGMENTS_IN_CACHE
                    && value < 3) {
                issues.add(new ValidationIssue(key, "value must be >= 3"));
            } else {
                normalized.put(key, Integer.valueOf(value));
            }
        }
        final Map<RuntimeSettingKey, Integer> effective = runtimeTuningState
                .previewEffective(normalized);
        final int segmentWriteCacheLimit = effective.get(
                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_ACTIVE_PARTITION)
                .intValue();
        final int maintenanceWriteCacheLimit = effective.get(
                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_PARTITION_BUFFER)
                .intValue();
        final int indexBuffer = effective
                .get(RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_INDEX_BUFFER)
                .intValue();
        if (maintenanceWriteCacheLimit <= segmentWriteCacheLimit) {
            issues.add(new ValidationIssue(
                    RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_PARTITION_BUFFER,
                    "value must be greater than maxNumberOfKeysInActivePartition"));
        }
        if (indexBuffer < maintenanceWriteCacheLimit) {
            issues.add(new ValidationIssue(
                    RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_INDEX_BUFFER,
                    "value must be >= maxNumberOfKeysInPartitionBuffer"));
        }
        return new RuntimePatchValidation(issues.isEmpty(), issues, normalized);
    }

    RuntimePatchResult apply(final RuntimeConfigPatch patch) {
        final RuntimePatchValidation validation = validate(patch);
        if (!validation.valid() || patch.dryRun()) {
            return new RuntimePatchResult(false, validation,
                    runtimeTuningState.snapshotCurrent());
        }
        final Map<RuntimeSettingKey, Integer> effective = runtimeTuningState
                .previewEffective(validation.normalizedValues());
        effectiveLimitsApplier.accept(effective);
        final ConfigurationSnapshot snapshot = runtimeTuningState
                .apply(validation.normalizedValues());
        if (validation.normalizedValues().containsKey(
                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_PARTITION_BEFORE_SPLIT)) {
            splitThresholdChangedListener.run();
        }
        return new RuntimePatchResult(true, validation, snapshot);
    }

    private final class IndexRuntimeViewImpl implements IndexRuntimeView {
        @Override
        public IndexRuntimeSnapshot snapshot() {
            return new IndexRuntimeSnapshot(conf.getIndexName(),
                    stateSupplier.get(), metricsSnapshotSupplier.get(),
                    Instant.now());
        }
    }

    private final class IndexConfigurationManagementImpl
            implements IndexConfigurationManagement {
        @Override
        public ConfigurationSnapshot getConfigurationActual() {
            return runtimeTuningState.snapshotCurrent();
        }

        @Override
        public ConfigurationSnapshot getConfigurationOriginal() {
            return runtimeTuningState.snapshotOriginal();
        }

        @Override
        public RuntimePatchValidation validate(final RuntimeConfigPatch patch) {
            return IndexRuntimeControlPlane.this.validate(patch);
        }

        @Override
        public RuntimePatchResult apply(final RuntimeConfigPatch patch) {
            return IndexRuntimeControlPlane.this.apply(patch);
        }
    }
}

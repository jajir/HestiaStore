package org.hestiastore.index.senku.internal;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;

/**
 * Coordinator-owned in-memory view of committed Senku sources.
 */
final class SenkuSourceCatalog {

    private static final Comparator<SenkuRunSource> RUN_ORDER = Comparator
            .comparingInt(SenkuRunSource::level)
            .thenComparingInt(SenkuRunSource::shardId)
            .thenComparingLong(SenkuRunSource::runId);

    private final Map<Long, Integer> flushPartCounts = new LinkedHashMap<>();
    private final Map<String, SenkuRunSource> runs = new LinkedHashMap<>();

    /**
     * Reconciles one metadata-only hierarchy observation with known sources.
     *
     * @param observedFlushes committed flush IDs and part counts
     * @param observedRuns committed sorted runs
     * @param reservedOutputPaths planned run paths not yet accepted
     */
    void reconcile(final Map<Long, Integer> observedFlushes,
            final Collection<SenkuRunSource> observedRuns,
            final Set<String> reservedOutputPaths) {
        final Map<Long, Integer> flushes = Vldtn.requireNonNull(
                observedFlushes, "observedFlushes");
        final Collection<SenkuRunSource> runSources = Vldtn.requireNonNull(
                observedRuns, "observedRuns");
        final Set<String> outputs = Vldtn.requireNonNull(reservedOutputPaths,
                "reservedOutputPaths");
        requireKnownFlushesUnchanged(flushes);
        requireKnownRunsUnchanged(runSources);
        flushPartCounts.putAll(flushes);
        for (final SenkuRunSource source : runSources) {
            final SenkuRunSource validated = Vldtn.requireNonNull(source,
                    "runSource");
            final String path = runPath(validated);
            if (!outputs.contains(path)) {
                runs.putIfAbsent(path, validated);
            }
        }
    }

    /**
     * Selects the oldest unreserved flush group.
     *
     * @param mergeFanIn configured fan-in
     * @param drain whether a partial group is eligible
     * @param reservedInputPaths exact already-reserved inputs
     * @return selected IDs or an empty array
     */
    long[] eligibleFlushIds(final int mergeFanIn, final boolean drain,
            final Set<String> reservedInputPaths) {
        final int fanIn = Vldtn.requireGreaterThanZero(mergeFanIn,
                "mergeFanIn");
        final Set<String> reserved = Vldtn.requireNonNull(reservedInputPaths,
                "reservedInputPaths");
        final long[] selected = flushPartCounts.keySet().stream().sorted()
                .filter(id -> !reserved.contains(flushPath(id))).limit(fanIn)
                .mapToLong(Long::longValue).toArray();
        return selected.length == fanIn || drain && selected.length > 0
                ? selected
                : new long[0];
    }

    /**
     * Selects the deterministic lowest-level unreserved sorted-run group.
     *
     * @param mergeFanIn configured fan-in
     * @param drain whether partial groups and promotion are eligible
     * @param activeSortedRunShards shards already queued or running
     * @param reservedInputPaths exact already-reserved inputs
     * @return selected sources or an empty list
     */
    List<SenkuRunSource> eligibleRunSources(final int mergeFanIn,
            final boolean drain, final Set<Integer> activeSortedRunShards,
            final Set<String> reservedInputPaths) {
        final int fanIn = Vldtn.requireGreaterThanZero(mergeFanIn,
                "mergeFanIn");
        final Set<Integer> activeShards = Vldtn.requireNonNull(
                activeSortedRunShards, "activeSortedRunShards");
        final Set<String> reserved = Vldtn.requireNonNull(reservedInputPaths,
                "reservedInputPaths");
        final List<SenkuRunSource> available = runs.values().stream()
                .filter(source -> !activeShards.contains(source.shardId()))
                .filter(source -> !reserved.contains(runPath(source)))
                .sorted(RUN_ORDER).toList();
        int start = 0;
        while (start < available.size()) {
            final SenkuRunSource first = available.get(start);
            int end = start + 1;
            while (end < available.size() && sameGroup(first,
                    available.get(end))) {
                end++;
            }
            final int count = end - start;
            if (count >= fanIn) {
                return List.copyOf(available.subList(start, start + fanIn));
            }
            if (drain && (count > 1
                    || hasHigherLevel(available, first.shardId(),
                            first.level()))) {
                return List.copyOf(available.subList(start, end));
            }
            start = end;
        }
        return List.of();
    }

    int flushCount() {
        return flushPartCounts.size();
    }

    int runCount() {
        return runs.size();
    }

    /**
     * Replaces accepted flush inputs with all completed L0 outputs.
     *
     * @param inputFlushIds exact accepted batch inputs
     * @param outputs completed L0 run sources
     */
    void acceptL0(final long[] inputFlushIds,
            final Collection<SenkuRunSource> outputs) {
        final long[] inputs = Vldtn.requireNonNull(inputFlushIds,
                "inputFlushIds");
        final Collection<SenkuRunSource> completed = Vldtn
                .requireNonNull(outputs, "outputs");
        for (final long input : inputs) {
            if (!flushPartCounts.containsKey(input)) {
                throw new IndexException("Unknown accepted flush input " + input
                        + ".");
            }
        }
        requireNewOutputs(completed);
        for (final long input : inputs) {
            flushPartCounts.remove(input);
        }
        addOutputs(completed);
    }

    /**
     * Replaces exact same-level run inputs with one completed output.
     *
     * @param inputs exact accepted run inputs
     * @param output completed replacement run
     */
    void acceptRunMerge(final Collection<SenkuRunSource> inputs,
            final SenkuRunSource output) {
        final Collection<SenkuRunSource> replaced = Vldtn.requireNonNull(inputs,
                "inputs");
        for (final SenkuRunSource input : replaced) {
            final String path = runPath(Vldtn.requireNonNull(input, "input"));
            if (!runs.containsKey(path)) {
                throw new IndexException("Unknown accepted run input '" + path
                        + "'.");
            }
        }
        final List<SenkuRunSource> outputs = List
                .of(Vldtn.requireNonNull(output, "output"));
        requireNewOutputs(outputs);
        for (final SenkuRunSource input : replaced) {
            runs.remove(runPath(input));
        }
        addOutputs(outputs);
    }

    long maximumRunId(final int shardId, final int level) {
        Vldtn.requireGreaterThanOrEqualToZero(shardId, "shardId");
        Vldtn.requireGreaterThanOrEqualToZero(level, "level");
        return runs.values().stream()
                .filter(source -> source.shardId() == shardId
                        && source.level() == level)
                .mapToLong(SenkuRunSource::runId).max().orElse(-1L);
    }

    int runCount(final int shardId) {
        Vldtn.requireGreaterThanOrEqualToZero(shardId, "shardId");
        return (int) runs.values().stream()
                .filter(source -> source.shardId() == shardId).count();
    }

    private void requireKnownFlushesUnchanged(
            final Map<Long, Integer> observed) {
        for (final Map.Entry<Long, Integer> known : flushPartCounts.entrySet()) {
            final Integer observedPartCount = observed.get(known.getKey());
            if (!known.getValue().equals(observedPartCount)) {
                throw new IndexException("Known flush source is missing or changed: "
                        + SenkuFileNames.flushDirectory(known.getKey()));
            }
        }
    }

    private void addOutputs(final Collection<SenkuRunSource> outputs) {
        for (final SenkuRunSource output : outputs) {
            final SenkuRunSource validated = Vldtn.requireNonNull(output,
                    "output");
            final String path = runPath(validated);
            runs.put(path, validated);
        }
    }

    private void requireNewOutputs(
            final Collection<SenkuRunSource> outputs) {
        final Set<String> newPaths = new HashSet<>();
        for (final SenkuRunSource output : outputs) {
            final String path = runPath(Vldtn.requireNonNull(output, "output"));
            if (runs.containsKey(path) || !newPaths.add(path)) {
                throw new IndexException("Run output already exists in catalog: "
                        + path);
            }
        }
    }

    private void requireKnownRunsUnchanged(
            final Collection<SenkuRunSource> observedSources) {
        final Map<String, SenkuRunSource> observed = new LinkedHashMap<>();
        for (final SenkuRunSource source : observedSources) {
            final String path = runPath(Vldtn.requireNonNull(source,
                    "runSource"));
            if (observed.put(path, source) != null) {
                throw new IndexException("Duplicate run source '" + path + "'.");
            }
        }
        for (final Map.Entry<String, SenkuRunSource> known : runs.entrySet()) {
            final SenkuRunSource current = observed.get(known.getKey());
            if (current == null || !sameManifest(known.getValue().manifest(),
                    current.manifest())) {
                throw new IndexException("Known run source is missing or changed: "
                        + known.getKey());
            }
        }
    }

    private static boolean sameManifest(final SenkuRunManifest first,
            final SenkuRunManifest second) {
        return first.partCount() == second.partCount()
                && first.recordCount() == second.recordCount();
    }

    private static boolean sameGroup(final SenkuRunSource first,
            final SenkuRunSource second) {
        return first.shardId() == second.shardId()
                && first.level() == second.level();
    }

    private static boolean hasHigherLevel(final List<SenkuRunSource> sources,
            final int shardId, final int level) {
        return sources.stream().anyMatch(source -> source.shardId() == shardId
                && source.level() > level);
    }

    static String flushPath(final long flushId) {
        return SenkuFileNames.FLUSH_DIRECTORY + "/"
                + SenkuFileNames.flushDirectory(flushId);
    }

    static String runPath(final SenkuRunSource source) {
        return SenkuFileNames.shardDirectory(source.shardId()) + "/"
                + SenkuFileNames.levelDirectory(source.level()) + "/"
                + SenkuFileNames.runDirectory(source.runId());
    }
}

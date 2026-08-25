package org.hestiastore.index.senku.internal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datablockfile.DataBlockSize;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.senku.SenkuMergeFunction;

/**
 * Single-thread-owned Senku maintenance catalog and scheduler state.
 */
final class SenkuMaintenanceCoordinator<K, V> {

    private final Directory rootDirectory;
    private final Directory flushDirectory;
    private final int shardCount;
    private final int mergeFanIn;
    private final SenkuSourceCatalog catalog = new SenkuSourceCatalog();
    private final Set<String> reservedInputPaths = new HashSet<>();
    private final Set<String> reservedOutputPaths = new HashSet<>();
    private final Set<Integer> activeSortedRunShards = new HashSet<>();
    private final Map<String, SenkuMergeReservation<K, V>> submitted =
            new LinkedHashMap<>();
    private final Map<String, Long> nextRunIds = new HashMap<>();
    private final Set<String> exhaustedRunIds = new HashSet<>();

    private final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;
    private final SenkuMergeFunction<K, V> mergeFunction;
    private final int maxKeysPerPage;
    private final long maxEntriesPerPart;
    private final DataBlockSize dataBlockSize;
    private final ThreadPoolExecutor workerExecutor;
    private final Executor controlExecutor;
    private final AtomicReference<IndexException> firstFailure;
    private final Consumer<Boolean> ingestionPauseConsumer;
    private final Consumer<IndexException> failureConsumer;
    private final Runnable completionConsumer;
    private final int submissionCapacity;

    private SenkuL0Batch activeL0Batch;

    /**
     * Creates metadata-only coordinator state for an existing writing layout.
     *
     * @param rootDirectory Senku index root
     * @param shardCount configured shard count
     * @param mergeFanIn configured merge fan-in
     */
    SenkuMaintenanceCoordinator(final Directory rootDirectory,
            final int shardCount, final int mergeFanIn) {
        this(rootDirectory, shardCount, mergeFanIn, null, null, null, 0, 0L,
                null, null, null, new AtomicReference<>(), paused -> {
                    // Metadata-only coordinator has no ingestion gate.
                }, failure -> {
                    // Metadata-only coordinator has no runtime failure owner.
                }, () -> {
                    // Metadata-only coordinator has no completion callback.
                });
    }

    /**
     * Creates the full bounded maintenance scheduler.
     *
     * @param rootDirectory Senku index root
     * @param shardCount configured shard count
     * @param mergeFanIn configured merge fan-in
     * @param keyTypeDescriptor key codec and comparator
     * @param valueTypeDescriptor value codec
     * @param mergeFunction duplicate reducer
     * @param maxKeysPerPage maximum output entries per page
     * @param maxEntriesPerPart maximum entries per physical part
     * @param dataBlockSize chunk-store block size
     * @param workerExecutor fixed bounded maintenance executor
     * @param controlExecutor serialized completion executor
     * @param firstFailure runtime first-failure holder
     * @param ingestionPauseConsumer high/low-water callback
     */
    SenkuMaintenanceCoordinator(final Directory rootDirectory,
            final int shardCount, final int mergeFanIn,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final SenkuMergeFunction<K, V> mergeFunction,
            final int maxKeysPerPage, final long maxEntriesPerPart,
            final DataBlockSize dataBlockSize,
            final ThreadPoolExecutor workerExecutor,
            final Executor controlExecutor,
            final AtomicReference<IndexException> firstFailure,
            final Consumer<Boolean> ingestionPauseConsumer) {
        this(rootDirectory, shardCount, mergeFanIn, keyTypeDescriptor,
                valueTypeDescriptor, mergeFunction, maxKeysPerPage,
                maxEntriesPerPart, dataBlockSize, workerExecutor,
                controlExecutor, firstFailure, ingestionPauseConsumer,
                failure -> {
                    // Tests may inspect firstFailure directly.
                }, () -> {
                    // Completion needs no callback outside a runtime.
                });
    }

    /**
     * Creates the full scheduler with lifecycle callbacks.
     *
     * @param failureConsumer first background failure callback
     * @param completionConsumer successful completion callback
     */
    SenkuMaintenanceCoordinator(final Directory rootDirectory,
            final int shardCount, final int mergeFanIn,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final SenkuMergeFunction<K, V> mergeFunction,
            final int maxKeysPerPage, final long maxEntriesPerPart,
            final DataBlockSize dataBlockSize,
            final ThreadPoolExecutor workerExecutor,
            final Executor controlExecutor,
            final AtomicReference<IndexException> firstFailure,
            final Consumer<Boolean> ingestionPauseConsumer,
            final Consumer<IndexException> failureConsumer,
            final Runnable completionConsumer) {
        this.rootDirectory = Vldtn.requireNonNull(rootDirectory,
                "rootDirectory");
        this.shardCount = Vldtn.requireGreaterThanZero(shardCount,
                "shardCount");
        this.mergeFanIn = Vldtn.requireGreaterThanZero(mergeFanIn,
                "mergeFanIn");
        if (!rootDirectory.isFileExists(SenkuFileNames.FLUSH_DIRECTORY)) {
            throw new IndexException("Missing structural flush directory.");
        }
        flushDirectory = rootDirectory
                .openSubDirectory(SenkuFileNames.FLUSH_DIRECTORY);
        this.keyTypeDescriptor = keyTypeDescriptor;
        this.valueTypeDescriptor = valueTypeDescriptor;
        this.mergeFunction = mergeFunction;
        this.maxKeysPerPage = maxKeysPerPage;
        this.maxEntriesPerPart = maxEntriesPerPart;
        this.dataBlockSize = dataBlockSize;
        this.workerExecutor = workerExecutor;
        this.controlExecutor = controlExecutor;
        this.firstFailure = Vldtn.requireNonNull(firstFailure, "firstFailure");
        this.ingestionPauseConsumer = Vldtn.requireNonNull(
                ingestionPauseConsumer, "ingestionPauseConsumer");
        this.failureConsumer = Vldtn.requireNonNull(failureConsumer,
                "failureConsumer");
        this.completionConsumer = Vldtn.requireNonNull(completionConsumer,
                "completionConsumer");
        if (workerExecutor == null) {
            submissionCapacity = 0;
        } else {
            Vldtn.requireNonNull(keyTypeDescriptor, "keyTypeDescriptor");
            Vldtn.requireNonNull(valueTypeDescriptor, "valueTypeDescriptor");
            Vldtn.requireNonNull(mergeFunction, "mergeFunction");
            Vldtn.requireGreaterThanZero(maxKeysPerPage, "maxKeysPerPage");
            Vldtn.requireGreaterThanZero(maxEntriesPerPart,
                    "maxEntriesPerPart");
            Vldtn.requireNonNull(dataBlockSize, "dataBlockSize");
            Vldtn.requireNonNull(controlExecutor, "controlExecutor");
            submissionCapacity = Math.addExact(
                    workerExecutor.getMaximumPoolSize(),
                    workerExecutor.getQueue().remainingCapacity()
                            + workerExecutor.getQueue().size());
        }
    }

    /**
     * Scans only structural directories and committed property metadata.
     */
    void scanOnce() {
        final Map<Long, Integer> flushes = scanFlushes();
        final List<SenkuRunSource> runs = scanRuns();
        catalog.reconcile(flushes, runs, reservedOutputPaths);
    }

    /**
     * Returns the current oldest eligible flush group.
     *
     * @param drain whether partial groups are eligible
     * @return selected flush IDs
     */
    long[] eligibleFlushIds(final boolean drain) {
        return catalog.eligibleFlushIds(mergeFanIn, drain, reservedInputPaths);
    }

    /**
     * Returns the current deterministic eligible sorted-run group.
     *
     * @param drain whether partial groups and promotions are eligible
     * @return selected run sources
     */
    List<SenkuRunSource> eligibleRunSources(final boolean drain) {
        return catalog.eligibleRunSources(mergeFanIn, drain,
                activeSortedRunShards, reservedInputPaths);
    }

    /**
     * Reconciles metadata and fills the bounded worker executor once.
     *
     * @param drain whether finalization eligibility rules apply
     */
    void scanAndScheduleOnce(final boolean drain) {
        requireScheduler();
        if (firstFailure.get() != null) {
            return;
        }
        try {
            scanOnce();
            fillCapacity(drain);
            updateBackpressure(drain);
        } catch (Exception e) {
            reportFailure(e);
        }
    }

    int submittedCount() {
        return submitted.size();
    }

    boolean hasActiveL0Batch() {
        return activeL0Batch != null;
    }

    int flushCount() {
        return catalog.flushCount();
    }

    int runCount() {
        return catalog.runCount();
    }

    private Map<Long, Integer> scanFlushes() {
        final Map<Long, Integer> observed = new LinkedHashMap<>();
        for (final String name : flushDirectory.getFileNames().toList()) {
            final long flushId = SenkuFileNames.parseFlushDirectory(name);
            final Directory source = flushDirectory.openSubDirectory(name);
            if (!source.isFileExists(SenkuFileNames.MANIFEST_FILE)) {
                continue;
            }
            final int partCount = SenkuMetadataCodec
                    .readFlushPartCount(source);
            if (observed.put(flushId, partCount) != null) {
                throw new IndexException("Duplicate flush ID " + flushId + ".");
            }
        }
        return observed;
    }

    private void fillCapacity(final boolean drain) {
        while (hasSubmissionCapacity() && firstFailure.get() == null) {
            if (activeL0Batch == null) {
                final long[] flushIds = eligibleFlushIds(drain);
                if (flushIds.length > 0) {
                    prepareL0Batch(flushIds);
                }
            }
            if (submitPendingL0()) {
                continue;
            }
            final List<SenkuRunSource> runInputs = eligibleRunSources(drain);
            if (runInputs.isEmpty()) {
                if (drain && submitMissingEmptyShard()) {
                    continue;
                }
                return;
            }
            submitRunMerge(runInputs);
        }
    }

    private void prepareL0Batch(final long[] flushIds) {
        final long[] outputIds = new long[shardCount];
        for (int shardId = 0; shardId < shardCount; shardId++) {
            outputIds[shardId] = allocateRunId(shardId, 0);
        }
        final SenkuL0Batch batch = SenkuL0Batch.load(flushDirectory, flushIds,
                outputIds, shardCount, maxEntriesPerPart, dataBlockSize);
        final List<String> flushPaths = Arrays.stream(flushIds)
                .mapToObj(SenkuSourceCatalog::flushPath).toList();
        requireUnreserved(flushPaths);
        reservedInputPaths.addAll(flushPaths);
        for (int shardId = 0; shardId < shardCount; shardId++) {
            final String path = runPath(shardId, 0, outputIds[shardId]);
            requireUnreserved(List.of(path));
            createRunDirectory(shardId, 0, outputIds[shardId]);
            reservedOutputPaths.add(path);
        }
        activeL0Batch = batch;
    }

    private boolean submitPendingL0() {
        if (activeL0Batch == null || !activeL0Batch.hasPendingShard()) {
            return false;
        }
        final int shardId = activeL0Batch.takeNextShard().orElseThrow();
        final long runId = activeL0Batch.outputRunId(shardId);
        final String outputPath = runPath(shardId, 0, runId);
        final SenkuMergeJob<K, V> job = new SenkuMergeJob<>(
                activeL0Batch.sourcesForShard(shardId),
                openRunDirectory(shardId, 0, runId), shardId, 0, runId,
                keyTypeDescriptor, valueTypeDescriptor, mergeFunction,
                maxKeysPerPage, maxEntriesPerPart, dataBlockSize,
                () -> firstFailure.get() == null);
        final List<String> inputs = Arrays
                .stream(activeL0Batch.inputFlushIds())
                .mapToObj(SenkuSourceCatalog::flushPath).toList();
        submit(SenkuMergeReservation.l0(job, inputs, outputPath));
        return true;
    }

    private void submitRunMerge(final List<SenkuRunSource> runInputs) {
        final SenkuRunSource first = runInputs.get(0);
        final int outputLevel;
        try {
            outputLevel = Math.incrementExact(first.level());
        } catch (ArithmeticException e) {
            throw new IndexException("Sorted-run level overflow.", e);
        }
        final long runId = allocateRunId(first.shardId(), outputLevel);
        final String outputPath = runPath(first.shardId(), outputLevel, runId);
        final List<String> inputPaths = runInputs.stream()
                .map(SenkuSourceCatalog::runPath).toList();
        requireUnreserved(inputPaths);
        requireUnreserved(List.of(outputPath));
        createRunDirectory(first.shardId(), outputLevel, runId);
        reservedInputPaths.addAll(inputPaths);
        reservedOutputPaths.add(outputPath);
        activeSortedRunShards.add(first.shardId());
        final SenkuMergeJob<K, V> guardedJob = SenkuMergeJob.forRuns(runInputs,
                openRunDirectory(first.shardId(), outputLevel, runId), runId,
                keyTypeDescriptor, valueTypeDescriptor, mergeFunction,
                maxKeysPerPage, maxEntriesPerPart, dataBlockSize,
                () -> firstFailure.get() == null);
        submit(SenkuMergeReservation.runs(guardedJob, runInputs, outputPath));
    }

    private void submit(final SenkuMergeReservation<K, V> reservation) {
        if (submitted.putIfAbsent(reservation.outputPath(), reservation) != null) {
            throw new IndexException("Merge output is already submitted: "
                    + reservation.outputPath());
        }
        try {
            workerExecutor.execute(() -> execute(reservation));
        } catch (RejectedExecutionException e) {
            submitted.remove(reservation.outputPath());
            throw new IndexException("Maintenance worker rejected a reserved job.",
                    e);
        }
    }

    private void execute(final SenkuMergeReservation<K, V> reservation) {
        if (firstFailure.get() != null) {
            return;
        }
        try {
            final SenkuCompletedRun completed = reservation.execute();
            if (firstFailure.get() == null) {
                controlExecutor.execute(
                        () -> processCompletion(reservation, completed));
            }
        } catch (Exception e) {
            reportFailure(e);
        }
    }

    private void processCompletion(
            final SenkuMergeReservation<K, V> reservation,
            final SenkuCompletedRun completed) {
        if (firstFailure.get() != null) {
            return;
        }
        try {
            final SenkuMergeReservation<K, V> known = submitted
                    .get(reservation.outputPath());
            if (known != reservation) {
                throw new IndexException("Unknown merge completion: "
                        + reservation.outputPath());
            }
            if (!reservation.outputPath().equals(runPath(completed.shardId(),
                    completed.level(), completed.runId()))) {
                throw new IndexException(
                        "Merge completion does not match its reservation.");
            }
            if (reservation.isL0()) {
                acceptL0Completion(reservation, completed);
            } else {
                acceptRunCompletion(reservation, completed);
            }
            completionConsumer.run();
        } catch (Exception e) {
            reportFailure(e);
        }
    }

    private void acceptL0Completion(
            final SenkuMergeReservation<K, V> reservation,
            final SenkuCompletedRun completed) {
        activeL0Batch.accept(completed);
        submitted.remove(reservation.outputPath());
        if (!activeL0Batch.isComplete()) {
            return;
        }
        final List<SenkuRunSource> outputs = new ArrayList<>(shardCount);
        for (int shardId = 0; shardId < shardCount; shardId++) {
            final SenkuCompletedRun output = activeL0Batch
                    .completedRun(shardId);
            outputs.add(new SenkuRunSource(
                    openRunDirectory(shardId, 0, output.runId()), shardId, 0,
                    output.runId(), output.manifest()));
        }
        final long[] flushIds = activeL0Batch.inputFlushIds();
        catalog.acceptL0(flushIds, outputs);
        for (final long flushId : flushIds) {
            deleteFlush(flushId);
            reservedInputPaths.remove(SenkuSourceCatalog.flushPath(flushId));
        }
        outputs.forEach(output -> reservedOutputPaths
                .remove(SenkuSourceCatalog.runPath(output)));
        activeL0Batch = null;
    }

    private void acceptRunCompletion(
            final SenkuMergeReservation<K, V> reservation,
            final SenkuCompletedRun completed) {
        final SenkuRunSource output = new SenkuRunSource(
                openRunDirectory(completed.shardId(), completed.level(),
                        completed.runId()),
                completed.shardId(), completed.level(), completed.runId(),
                completed.manifest());
        catalog.acceptRunMerge(reservation.runInputs(), output);
        for (final SenkuRunSource input : reservation.runInputs()) {
            deleteRun(input);
            reservedInputPaths.remove(SenkuSourceCatalog.runPath(input));
        }
        reservedOutputPaths.remove(reservation.outputPath());
        activeSortedRunShards.remove(completed.shardId());
        submitted.remove(reservation.outputPath());
    }

    private void updateBackpressure(final boolean drain) {
        final boolean queueFull = workerExecutor.getQueue()
                .remainingCapacity() == 0;
        final boolean pendingL0 = activeL0Batch != null
                && activeL0Batch.hasPendingShard();
        final boolean eligible = pendingL0
                || eligibleFlushIds(drain).length > 0
                || !eligibleRunSources(drain).isEmpty();
        ingestionPauseConsumer.accept(queueFull && eligible);
    }

    private boolean submitMissingEmptyShard() {
        if (activeL0Batch != null || catalog.flushCount() > 0) {
            return false;
        }
        for (int shardId = 0; shardId < shardCount; shardId++) {
            if (catalog.runCount(shardId) == 0
                    && !activeSortedRunShards.contains(shardId)) {
                final long runId = allocateRunId(shardId, 0);
                final String outputPath = runPath(shardId, 0, runId);
                requireUnreserved(List.of(outputPath));
                final Directory output = createRunDirectory(shardId, 0, runId);
                reservedOutputPaths.add(outputPath);
                activeSortedRunShards.add(shardId);
                final SenkuMergeJob<K, V> job = new SenkuMergeJob<>(List.of(),
                        output, shardId, 0, runId, keyTypeDescriptor,
                        valueTypeDescriptor, mergeFunction, maxKeysPerPage,
                        maxEntriesPerPart, dataBlockSize,
                        () -> firstFailure.get() == null);
                submit(SenkuMergeReservation.runs(job, List.of(), outputPath));
                return true;
            }
        }
        return false;
    }

    boolean isDrainComplete() {
        if (firstFailure.get() != null || activeL0Batch != null
                || !submitted.isEmpty() || catalog.flushCount() != 0) {
            return false;
        }
        for (int shardId = 0; shardId < shardCount; shardId++) {
            if (catalog.runCount(shardId) != 1) {
                return false;
            }
        }
        return true;
    }

    private boolean hasSubmissionCapacity() {
        return submitted.size() < submissionCapacity;
    }

    private long allocateRunId(final int shardId, final int level) {
        final String key = shardId + "/" + level;
        if (exhaustedRunIds.contains(key)) {
            throw new IndexException("Senku run ID sequence is exhausted.");
        }
        final long firstAvailable = nextRunIds.computeIfAbsent(key,
                ignored -> nextAfter(catalog.maximumRunId(shardId, level)));
        if (firstAvailable == Long.MAX_VALUE) {
            exhaustedRunIds.add(key);
        } else {
            nextRunIds.put(key, firstAvailable + 1L);
        }
        return firstAvailable;
    }

    private static long nextAfter(final long current) {
        if (current == Long.MAX_VALUE) {
            throw new IndexException("Senku run ID sequence is exhausted.");
        }
        return current + 1L;
    }

    private Directory createRunDirectory(final int shardId, final int level,
            final long runId) {
        final String shardName = SenkuFileNames.shardDirectory(shardId);
        if (!rootDirectory.isFileExists(shardName)) {
            rootDirectory.mkdir(shardName);
        }
        final Directory shard = rootDirectory.openSubDirectory(shardName);
        final String levelName = SenkuFileNames.levelDirectory(level);
        if (!shard.isFileExists(levelName)) {
            shard.mkdir(levelName);
        }
        final Directory levelDirectory = shard.openSubDirectory(levelName);
        final String runName = SenkuFileNames.runDirectory(runId);
        if (!levelDirectory.mkdir(runName)) {
            throw new IndexException("Planned run directory already exists: "
                    + runPath(shardId, level, runId));
        }
        return levelDirectory.openSubDirectory(runName);
    }

    private Directory openRunDirectory(final int shardId, final int level,
            final long runId) {
        return rootDirectory
                .openSubDirectory(SenkuFileNames.shardDirectory(shardId))
                .openSubDirectory(SenkuFileNames.levelDirectory(level))
                .openSubDirectory(SenkuFileNames.runDirectory(runId));
    }

    private void deleteFlush(final long flushId) {
        final String name = SenkuFileNames.flushDirectory(flushId);
        deleteLeaf(flushDirectory.openSubDirectory(name));
        if (!flushDirectory.rmdir(name)) {
            throw new IndexException("Missing obsolete flush directory '" + name
                    + "'.");
        }
    }

    private void deleteRun(final SenkuRunSource source) {
        deleteLeaf(source.directory());
        final String shardName = SenkuFileNames.shardDirectory(source.shardId());
        final Directory shard = rootDirectory.openSubDirectory(shardName);
        final String levelName = SenkuFileNames.levelDirectory(source.level());
        final Directory levelDirectory = shard.openSubDirectory(levelName);
        final String runName = SenkuFileNames.runDirectory(source.runId());
        if (!levelDirectory.rmdir(runName)) {
            throw new IndexException("Missing obsolete run directory '"
                    + SenkuSourceCatalog.runPath(source) + "'.");
        }
        if (levelDirectory.getFileNames().findAny().isEmpty()) {
            shard.rmdir(levelName);
        }
        if (shard.getFileNames().findAny().isEmpty()) {
            rootDirectory.rmdir(shardName);
        }
    }

    private static void deleteLeaf(final Directory directory) {
        for (final String fileName : directory.getFileNames().toList()) {
            if (!directory.deleteFile(fileName)) {
                throw new IndexException("Missing obsolete source file '"
                        + fileName + "'.");
            }
        }
    }

    private void requireUnreserved(final List<String> paths) {
        for (final String path : paths) {
            if (reservedInputPaths.contains(path)
                    || reservedOutputPaths.contains(path)) {
                throw new IndexException("Merge path is already reserved: "
                        + path);
            }
        }
    }

    private void reportFailure(final Exception failure) {
        final IndexException indexFailure = failure instanceof IndexException
                ? (IndexException) failure
                : new IndexException("Senku maintenance failed.", failure);
        if (firstFailure.compareAndSet(null, indexFailure)) {
            failureConsumer.accept(indexFailure);
        }
    }

    private void requireScheduler() {
        if (workerExecutor == null) {
            throw new IndexException("Maintenance scheduler is not configured.");
        }
    }

    private static String runPath(final int shardId, final int level,
            final long runId) {
        return SenkuFileNames.shardDirectory(shardId) + "/"
                + SenkuFileNames.levelDirectory(level) + "/"
                + SenkuFileNames.runDirectory(runId);
    }

    private List<SenkuRunSource> scanRuns() {
        final List<SenkuRunSource> observed = new ArrayList<>();
        for (final String rootName : rootDirectory.getFileNames().toList()) {
            if (SenkuFileNames.LOCK_FILE.equals(rootName)
                    || SenkuFileNames.FLUSH_DIRECTORY.equals(rootName)) {
                continue;
            }
            final int shardId = SenkuFileNames.parseShardDirectory(rootName);
            if (shardId >= shardCount) {
                throw new IndexException("Unexpected shard ID " + shardId + ".");
            }
            scanShard(rootName, shardId, observed);
        }
        return observed;
    }

    private void scanShard(final String shardName, final int shardId,
            final List<SenkuRunSource> observed) {
        final Directory shard = rootDirectory.openSubDirectory(shardName);
        for (final String levelName : shard.getFileNames().toList()) {
            final int level = SenkuFileNames.parseLevelDirectory(levelName);
            final Directory levelDirectory = shard.openSubDirectory(levelName);
            for (final String runName : levelDirectory.getFileNames().toList()) {
                final long runId = SenkuFileNames.parseRunDirectory(runName);
                final Directory runDirectory = levelDirectory
                        .openSubDirectory(runName);
                if (runDirectory.isFileExists(SenkuFileNames.MANIFEST_FILE)) {
                    observed.add(new SenkuRunSource(runDirectory, shardId,
                            level, runId, SenkuMetadataCodec
                                    .readRunManifest(runDirectory)));
                }
            }
        }
    }
}

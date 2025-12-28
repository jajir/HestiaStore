package org.hestiastore.index.segment;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.EntryIteratorWithLock;
import org.hestiastore.index.EntryWriter;
import org.hestiastore.index.OptimisticLock;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.WriteTransaction;
import org.hestiastore.index.WriteTransaction.WriterFunction;
import org.hestiastore.index.directory.FileReaderSeekable;
import org.hestiastore.index.directory.async.AsyncFileReaderSeekableBlockingAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A single on-disk index segment with delta-cache and compaction support.
 * <p>
 * Segment coordinates read and write operations for a bounded subset of the
 * index data. It encapsulates the underlying files, provides search and
 * iteration, accepts writes through a delta cache (with o̦ptional automatic
 * compaction), and exposes utilities for statistics, consistency checking, and
 * splitting oversized segments. Versioning is tracked via an optimistic lock to
 * guard concurrent readers while updates occur.
 *
 * @author honza
 *
 * @param <K> key type stored in this segment
 * @param <V> value type stored in this segment
 */
public class SegmentImpl<K, V> extends AbstractCloseableResource
        implements Segment<K, V> {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final SegmentConf segmentConf;
    private final SegmentFiles<K, V> segmentFiles;
    private final VersionController versionController;
    private final SegmentPropertiesManager segmentPropertiesManager;
    private final SegmentCompacter<K, V> segmentCompacter;
    private final SegmentDeltaCacheController<K, V> deltaCacheController;
    private final SegmentSearcher<K, V> segmentSearcher;
    private final SegmentResources<K, V> segmentResources;
    private final SegmentSplitter<K, V> segmentSplitter;
    private final SegmentSplitterPolicy<K, V> segmentSplitterPolicy;
    private final SegmentCompactionPolicyWithManager segmentCompactionPolicy;
    private SegmentIndexSearcher<K, V> segmentIndexSearcher;
    private FileReaderSeekable seekableReader;

    // Reduced constructors: keep only the most complex constructor below.

    /**
     * Full DI constructor allowing to inject both compacter and replacer.
     * Useful for testing and advanced wiring.
     */
    public SegmentImpl(final SegmentFiles<K, V> segmentFiles,
            final SegmentConf segmentConf,
            final VersionController versionController,
            final SegmentPropertiesManager segmentPropertiesManager,
            final SegmentResources<K, V> segmentResources,
            final SegmentDeltaCacheController<K, V> segmentDeltaCacheController,
            final SegmentSearcher<K, V> segmentSearcher,
            final SegmentCompactionPolicyWithManager segmentCompactionPolicy,
            final SegmentCompacter<K, V> segmentCompacter,
            final SegmentReplacer<K, V> segmentReplacer,
            final SegmentSplitterPolicy<K, V> segmentSplitterPolicy) {
        this.segmentConf = Vldtn.requireNonNull(segmentConf, "segmentConf");
        this.segmentFiles = Vldtn.requireNonNull(segmentFiles, "segmentFiles");
        logger.debug("Initializing segment '{}'", segmentFiles.getId());
        this.versionController = Vldtn.requireNonNull(versionController,
                "versionController");
        this.segmentResources = Vldtn.requireNonNull(segmentResources,
                "segmentResources");
        this.segmentPropertiesManager = Vldtn.requireNonNull(
                segmentPropertiesManager, "segmentPropertiesManager");
        this.deltaCacheController = Vldtn.requireNonNull(
                segmentDeltaCacheController, "segmentDeltaCacheController");
        this.segmentCompacter = Vldtn.requireNonNull(segmentCompacter,
                "segmentCompacter");
        this.segmentCompactionPolicy = Vldtn.requireNonNull(
                segmentCompactionPolicy, "segmentCompactionPolicy");
        this.segmentSearcher = Vldtn.requireNonNull(segmentSearcher,
                "segmentSearcher");
        final SegmentSplitterPolicy<K, V> validatedSplitterPolicy = Vldtn
                .requireNonNull(segmentSplitterPolicy, "segmentSplitterPolicy");
        this.segmentSplitterPolicy = validatedSplitterPolicy;
        final SegmentReplacer<K, V> injectedReplacer = Vldtn
                .requireNonNull(segmentReplacer, "segmentReplacer");
        this.segmentSplitter = new SegmentSplitter<>(this, versionController,
                injectedReplacer);
    }

    @Override
    public SegmentStats getStats() {
        return segmentPropertiesManager.getSegmentStats();
    }

    @Override
    public long getNumberOfKeys() {
        return segmentPropertiesManager.getSegmentStats().getNumberOfKeys();
    }

    @Override
    public K checkAndRepairConsistency() {
        final SegmentConsistencyChecker<K, V> consistencyChecker = new SegmentConsistencyChecker<>(
                this, segmentFiles.getKeyTypeDescriptor().getComparator());
        return consistencyChecker.checkAndRepairConsistency();
    }

    @Override
    public void invalidateIterators() {
        versionController.changeVersion();
    }

    @Override
    public EntryIterator<K, V> openIterator() {
        final EntryIterator<K, V> mergedEntryIterator = new MergeDeltaCacheWithIndexIterator<>(
                segmentFiles.getIndexFile().openIterator(),
                segmentFiles.getKeyTypeDescriptor(),
                segmentFiles.getValueTypeDescriptor(),
                deltaCacheController.getDeltaCache().getAsSortedList());
        return new EntryIteratorWithLock<>(mergedEntryIterator,
                new OptimisticLock(versionController), getId().toString());
    }

    @Override
    public void forceCompact() {
        segmentCompacter.forceCompact(this);
    }

    @Override
    public void optionallyCompact() {
        segmentCompacter.optionallyCompact(this);
    }

    /**
     * Method should be called just from inside of this package. Method open
     * direct writer to scarce index and main sst file.
     * 
     * Writer should be opend and closed as one atomic operation.
     * 
     * @return return segment writer object
     */
    void executeFullWriteTx(final WriterFunction<K, V> writeFunction) {
        openFullWriteTx().execute(writeFunction);
    }

    WriteTransaction<K, V> openFullWriteTx() {
        return new SegmentFullWriterTx<>(segmentFiles, segmentPropertiesManager,
                segmentConf.getMaxNumberOfKeysInChunk(), segmentResources,
                deltaCacheController);
    }

    /**
     * Allows to open writer that will write to delta cache. When number of keys
     * in segment exceeds certain threshold, delta cache will be flushed to
     * disk.
     * 
     * It's not necesarry to run it in transaction because it's always new file.
     */
    @Override
    public EntryWriter<K, V> openDeltaCacheWriter() {
        versionController.changeVersion();
        return new SegmentDeltaCacheCompactingWriter<>(this, segmentCompacter,
                deltaCacheController, segmentCompactionPolicy);
    }

    @Override
    public V get(final K key) {
        return segmentSearcher.get(key, segmentResources,
                getSegmentIndexSearcher());
    }

    /**
     * Creates a new, empty segment using the same configuration and type
     * descriptors as this segment, bound to the provided id.
     * <p>
     * Only configuration/state is copied (directory, key/value descriptors, and
     * a copied {@link SegmentConf}). No data is cloned.
     *
     * @param segmentId required id for the new sibling segment
     * @return a new segment sharing the same configuration
     */
    @Override
    public SegmentImpl<K, V> createSegmentWithSameConfig(SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        return Segment.<K, V>builder()
                .withAsyncDirectory(segmentFiles.getAsyncDirectory())
                .withId(segmentId)
                .withKeyTypeDescriptor(segmentFiles.getKeyTypeDescriptor())
                .withValueTypeDescriptor(segmentFiles.getValueTypeDescriptor())
                .withSegmentConf(new SegmentConf(segmentConf)).build();
    }

    /**
     * Splits this segment into two parts using a precomputed plan. Callers are
     * expected to decide when to split (e.g., via
     * {@link #getSegmentSplitterPolicy()} and index configuration) and then
     * invoke this method with a newly allocated {@link SegmentId} for the
     * lower half.
     *
     * @param segmentId id for the new lower segment
     * @param plan precomputed split plan
     * @return result of the split operation
     */
    @Override
    public SegmentSplitterResult<K, V> split(final SegmentId segmentId,
            final SegmentSplitterPlan<K, V> plan) {
        return segmentSplitter.split(segmentId, plan);
    }

    /**
     * Returns the policy object that estimates the effective number of keys in
     * this segment (on-disk + delta cache) and advises whether a compaction
     * should take place before attempting to split.
     * <p>
     * Typical usage is to create a {@link SegmentSplitterPlan} from this
     * policy, evaluate whether the split should occur based on index limits,
     * and only then execute the split via {@link #split(SegmentId, SegmentSplitterPlan)}.
     *
     * @return the splitter policy associated with this segment
     */
    @Override
    public SegmentSplitterPolicy<K, V> getSegmentSplitterPolicy() {
        return segmentSplitterPolicy;
    }

    @Override
    protected void doClose() {
        resetSegmentIndexSearcher();
        logger.debug("Closing segment '{}'", segmentFiles.getId());
    }

    @Override
    public SegmentId getId() {
        return segmentFiles.getId();
    }

    @Override
    public int getVersion() {
        return versionController.getVersion();
    }

    public SegmentFiles<K, V> getSegmentFiles() {
        return segmentFiles;
    }

    public SegmentConf getSegmentConf() {
        return segmentConf;
    }

    public SegmentPropertiesManager getSegmentPropertiesManager() {
        return segmentPropertiesManager;
    }

    SegmentIndexSearcher<K, V> getSegmentIndexSearcher() {
        if (segmentIndexSearcher == null) {
            segmentIndexSearcher = new SegmentIndexSearcher<>(
                    segmentFiles.getIndexFile(),
                    segmentConf.getMaxNumberOfKeysInChunk(),
                    segmentFiles.getKeyTypeDescriptor().getComparator(),
                    getSeekableReader());
        }
        return segmentIndexSearcher;
    }

    FileReaderSeekable getSeekableReader() {
        if (seekableReader == null) {
            final String indexFileName = segmentFiles.getIndexFileName();
            if (segmentFiles.getAsyncDirectory()
                    .isFileExistsAsync(indexFileName).toCompletableFuture()
                    .join()) {
                seekableReader = new AsyncFileReaderSeekableBlockingAdapter(
                        segmentFiles.getAsyncDirectory()
                                .getFileReaderSeekableAsync(indexFileName)
                                .toCompletableFuture().join());
            }
        }
        return seekableReader;
    }

    void resetSegmentIndexSearcher() {
        if (segmentIndexSearcher != null) {
            segmentIndexSearcher.close();
            segmentIndexSearcher = null;
        }
        resetSeekableReader();
    }

    void resetSeekableReader() {
        if (seekableReader != null) {
            seekableReader.close();
            seekableReader = null;
        }
    }

}

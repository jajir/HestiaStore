# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `40.447 ops/s` | `54.898 ops/s` | `+35.73%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `41.785 ops/s` | `41.187 ops/s` | `-1.43%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `172318.153 ops/s` | `184096.292 ops/s` | `+6.84%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `3557712.582 ops/s` | `3633713.021 ops/s` | `+2.14%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `84.995 ops/s` | `94.636 ops/s` | `+11.34%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `86.270 ops/s` | `81.484 ops/s` | `-5.55%` | `warning` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `172470.393 ops/s` | `182912.192 ops/s` | `+6.05%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3779533.939 ops/s` | `3665597.617 ops/s` | `-3.01%` | `warning` |
| `segment-index-get-overlay:getHitAsyncJoin` | `165768.014 ops/s` | `185771.090 ops/s` | `+12.07%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4346780.419 ops/s` | `3690168.288 ops/s` | `-15.11%` | `worse` |
| `segment-index-get-overlay:getMissAsyncJoin` | `166403.635 ops/s` | `182802.334 ops/s` | `+9.85%` | `better` |
| `segment-index-get-overlay:getMissSync` | `3532611.567 ops/s` | `3662356.741 ops/s` | `+3.67%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `63174.844 ops/s` | `63125.046 ops/s` | `-0.08%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `116089.197 ops/s` | `112857.675 ops/s` | `-2.78%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `165419.812 ops/s` | `177617.013 ops/s` | `+7.37%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3798832.844 ops/s` | `3365987.223 ops/s` | `-11.39%` | `worse` |
| `segment-index-hot-partition-put:putHotPartition` | `2641763.342 ops/s` | `2353513.792 ops/s` | `-10.91%` | `worse` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1651360.997 ops/s` | `1355502.169 ops/s` | `-17.92%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `243.782 ms/op` | `278.221 ms/op` | `+14.13%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `265.507 ms/op` | `302.197 ms/op` | `+13.82%` | `better` |
| `segment-index-lifecycle:openExisting` | `241.667 ms/op` | `277.958 ms/op` | `+15.02%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `541911.826 ops/s` | `497963.622 ops/s` | `-8.11%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `536596.559 ops/s` | `492597.806 ops/s` | `-8.20%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5315.268 ops/s` | `5365.816 ops/s` | `+0.95%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `301581.415 ops/s` | `299423.976 ops/s` | `-0.72%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `265599.401 ops/s` | `264824.238 ops/s` | `-0.29%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `35982.014 ops/s` | `34599.737 ops/s` | `-3.84%` | `warning` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `1978.203 ops/s` | `2490.226 ops/s` | `+25.88%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2213.232 ops/s` | `2742.458 ops/s` | `+23.91%` | `better` |
| `segment-index-persisted-mutation:putAsyncJoin` | `1957.470 ops/s` | `2502.605 ops/s` | `+27.85%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2137.225 ops/s` | `2667.355 ops/s` | `+24.80%` | `better` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8335604.694 ops/s` | `8103090.528 ops/s` | `-2.79%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7938132.814 ops/s` | `7759428.817 ops/s` | `-2.25%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `9075390.755 ops/s` | `8601164.853 ops/s` | `-5.23%` | `warning` |
| `sorted-data-diff-key-read-large:readNextKey` | `7413420.732 ops/s` | `6660473.562 ops/s` | `-10.16%` | `worse` |

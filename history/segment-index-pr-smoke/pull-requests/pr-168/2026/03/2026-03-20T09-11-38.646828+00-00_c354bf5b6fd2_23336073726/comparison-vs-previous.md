# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `f110d0a08518e6d6f918649d4aa14d48cfb15719`
- Candidate SHA: `c354bf5b6fd28fe3c018def1a335d535975dc6bd`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `94.222 ops/s` | `98.882 ops/s` | `+4.95%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `86.834 ops/s` | `93.988 ops/s` | `+8.24%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `174459.557 ops/s` | `174870.428 ops/s` | `+0.24%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3865467.232 ops/s` | `3830612.440 ops/s` | `-0.90%` | `neutral` |
| `segment-index-get-overlay:getHitAsyncJoin` | `171076.183 ops/s` | `171424.306 ops/s` | `+0.20%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `4076722.394 ops/s` | `4036595.057 ops/s` | `-0.98%` | `neutral` |
| `segment-index-get-overlay:getMissAsyncJoin` | `174226.430 ops/s` | `176589.268 ops/s` | `+1.36%` | `neutral` |
| `segment-index-get-overlay:getMissSync` | `3937413.993 ops/s` | `3957150.965 ops/s` | `+0.50%` | `neutral` |
| `segment-index-get-persisted:getHitAsyncJoin` | `56471.502 ops/s` | `54947.470 ops/s` | `-2.70%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `105852.644 ops/s` | `102240.121 ops/s` | `-3.41%` | `warning` |
| `segment-index-get-persisted:getMissAsyncJoin` | `173397.046 ops/s` | `176443.267 ops/s` | `+1.76%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `4042660.628 ops/s` | `4185696.860 ops/s` | `+3.54%` | `better` |
| `segment-index-hot-partition-put:putHotPartition` | `3107395.584 ops/s` | `3079742.146 ops/s` | `-0.89%` | `neutral` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1645154.734 ops/s` | `1607176.720 ops/s` | `-2.31%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `417384.635 ops/s` | `442457.980 ops/s` | `+6.01%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `412257.912 ops/s` | `437256.831 ops/s` | `+6.06%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5126.723 ops/s` | `5201.148 ops/s` | `+1.45%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `200172.149 ops/s` | `194744.738 ops/s` | `-2.71%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `197546.142 ops/s` | `192019.936 ops/s` | `-2.80%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `2626.006 ops/s` | `2724.802 ops/s` | `+3.76%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `1572.623 ops/s` | `1400.442 ops/s` | `-10.95%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `1761.625 ops/s` | `1559.803 ops/s` | `-11.46%` | `worse` |
| `segment-index-persisted-mutation:putAsyncJoin` | `1369.412 ops/s` | `1525.198 ops/s` | `+11.38%` | `better` |
| `segment-index-persisted-mutation:putSync` | `1587.371 ops/s` | `1574.890 ops/s` | `-0.79%` | `neutral` |

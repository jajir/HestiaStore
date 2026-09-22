# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4845735.414 ops/s` | `4861775.811 ops/s` | `+0.33%` | `neutral` |
| `segment-index-get-live:getMissSync` | `4513038.503 ops/s` | `5053263.471 ops/s` | `+11.97%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `277108.721 ops/s` | `278522.897 ops/s` | `+0.51%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `4561316.372 ops/s` | `4546816.362 ops/s` | `-0.32%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `3604056.359 ops/s` | `3628287.717 ops/s` | `+0.67%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `4627621.498 ops/s` | `4678917.495 ops/s` | `+1.11%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `3594021.202 ops/s` | `3510311.456 ops/s` | `-2.33%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `4773420.417 ops/s` | `5007874.209 ops/s` | `+4.91%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `4579821.446 ops/s` | `4283421.186 ops/s` | `-6.47%` | `warning` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2288535.077 ops/s` | `2301328.384 ops/s` | `+0.56%` | `neutral` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `245.526 ms/op` | `239.311 ms/op` | `-2.53%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `260.840 ms/op` | `256.752 ms/op` | `-1.57%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `239.211 ms/op` | `236.631 ms/op` | `-1.08%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `558265.706 ops/s` | `558194.207 ops/s` | `-0.01%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `263269.061 ops/s` | `259427.128 ops/s` | `-1.46%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `294996.644 ops/s` | `298767.079 ops/s` | `+1.28%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1237555.031 ops/s` | `1275527.997 ops/s` | `+3.07%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1202014.043 ops/s` | `1241610.469 ops/s` | `+3.29%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `35540.987 ops/s` | `33917.529 ops/s` | `-4.57%` | `warning` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `6985.580 ops/s` | `4317.202 ops/s` | `-38.20%` | `worse` |
| `segment-index-persisted-mutation-concurrent:putSync` | `7084.957 ops/s` | `4600.747 ops/s` | `-35.06%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `2648.800 ops/s` | `1788.939 ops/s` | `-32.46%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `2562.466 ops/s` | `1742.404 ops/s` | `-32.00%` | `worse` |
| `segment-index-range-scan:boundedScan` | `28.212 us/op` | `33.153 us/op` | `+17.51%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2029.261 us/op` | `2004.221 us/op` | `-1.23%` | `neutral` |
| `segment-index-range-scan:sequentialRead` | `3824.373 us/op` | `3778.021 us/op` | `-1.21%` | `neutral` |
| `segment-merge-sequential:mergeSequential` | `333.836 us/op` | `331.308 us/op` | `-0.76%` | `neutral` |

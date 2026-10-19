# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `6846360.469 ops/s` | `6392803.869 ops/s` | `-6.62%` | `warning` |
| `segment-index-get-live:getMissSync` | `6694866.771 ops/s` | `6822561.616 ops/s` | `+1.91%` | `neutral` |
| `segment-index-get-multisegment-cold:getHitSync` | `519222.829 ops/s` | `512362.036 ops/s` | `-1.32%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `6713718.945 ops/s` | `7123153.696 ops/s` | `+6.10%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `5625555.367 ops/s` | `5688281.599 ops/s` | `+1.12%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `6359223.349 ops/s` | `6349925.919 ops/s` | `-0.15%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `5065422.024 ops/s` | `5183059.094 ops/s` | `+2.32%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `6817734.259 ops/s` | `6801838.644 ops/s` | `-0.23%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `5837443.296 ops/s` | `6702548.147 ops/s` | `+14.82%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `3095268.028 ops/s` | `3484457.201 ops/s` | `+12.57%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `203.048 ms/op` | `199.965 ms/op` | `-1.52%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `217.911 ms/op` | `215.613 ms/op` | `-1.05%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `204.653 ms/op` | `204.719 ms/op` | `+0.03%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `723381.676 ops/s` | `720784.300 ops/s` | `-0.36%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `437317.395 ops/s` | `432936.779 ops/s` | `-1.00%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `286064.280 ops/s` | `287847.521 ops/s` | `+0.62%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1756472.221 ops/s` | `1869981.865 ops/s` | `+6.46%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1703403.617 ops/s` | `1817994.794 ops/s` | `+6.73%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `53068.603 ops/s` | `51987.072 ops/s` | `-2.04%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `8213.282 ops/s` | `7819.730 ops/s` | `-4.79%` | `warning` |
| `segment-index-persisted-mutation-concurrent:putSync` | `8828.742 ops/s` | `8200.343 ops/s` | `-7.12%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `3766.719 ops/s` | `3763.363 ops/s` | `-0.09%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `3632.908 ops/s` | `3710.498 ops/s` | `+2.14%` | `neutral` |
| `segment-index-range-scan:boundedScan` | `17.983 us/op` | `16.896 us/op` | `-6.04%` | `warning` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1169.184 us/op` | `1180.993 us/op` | `+1.01%` | `neutral` |
| `segment-index-range-scan:sequentialRead` | `2286.340 us/op` | `2286.547 us/op` | `+0.01%` | `neutral` |
| `segment-merge-sequential:mergeSequential` | `181.248 us/op` | `179.766 us/op` | `-0.82%` | `neutral` |

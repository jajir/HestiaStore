# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4570328.413 ops/s` | `4818178.962 ops/s` | `+5.42%` | `better` |
| `segment-index-get-live:getMissSync` | `4819524.501 ops/s` | `4507500.530 ops/s` | `-6.47%` | `warning` |
| `segment-index-get-multisegment-cold:getHitSync` | `271516.158 ops/s` | `285000.144 ops/s` | `+4.97%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `4901923.213 ops/s` | `4802291.264 ops/s` | `-2.03%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `3504695.714 ops/s` | `3813366.795 ops/s` | `+8.81%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `4419793.553 ops/s` | `4456845.390 ops/s` | `+0.84%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `3490357.312 ops/s` | `3396190.865 ops/s` | `-2.70%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `4586927.537 ops/s` | `4757754.978 ops/s` | `+3.72%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `4498626.545 ops/s` | `4405813.587 ops/s` | `-2.06%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2237791.684 ops/s` | `2332192.788 ops/s` | `+4.22%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `245.425 ms/op` | `244.983 ms/op` | `-0.18%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `259.060 ms/op` | `261.492 ms/op` | `+0.94%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `242.243 ms/op` | `242.122 ms/op` | `-0.05%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `550011.892 ops/s` | `553380.699 ops/s` | `+0.61%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `258586.263 ops/s` | `261098.804 ops/s` | `+0.97%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `291425.630 ops/s` | `292281.895 ops/s` | `+0.29%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1215560.545 ops/s` | `1200899.528 ops/s` | `-1.21%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1180648.845 ops/s` | `1165782.735 ops/s` | `-1.26%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `34911.700 ops/s` | `35116.793 ops/s` | `+0.59%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `6817.221 ops/s` | `6881.026 ops/s` | `+0.94%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:putSync` | `7183.502 ops/s` | `6839.123 ops/s` | `-4.79%` | `warning` |
| `segment-index-persisted-mutation:deleteSync` | `2642.936 ops/s` | `2443.344 ops/s` | `-7.55%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `2562.378 ops/s` | `2428.557 ops/s` | `-5.22%` | `warning` |
| `segment-index-range-scan:boundedScan` | `26.033 us/op` | `25.829 us/op` | `-0.78%` | `neutral` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2013.326 us/op` | `1998.195 us/op` | `-0.75%` | `neutral` |
| `segment-index-range-scan:sequentialRead` | `3885.004 us/op` | `4007.920 us/op` | `+3.16%` | `better` |
| `segment-merge-sequential:mergeSequential` | `333.035 us/op` | `333.172 us/op` | `+0.04%` | `neutral` |

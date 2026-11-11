# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2853679.229 ops/s` | `4704764.547 ops/s` | `+64.87%` | `better` |
| `segment-index-get-live:getMissSync` | `2991133.759 ops/s` | `4190147.014 ops/s` | `+40.09%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `247883.065 ops/s` | `285988.413 ops/s` | `+15.37%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `2775828.922 ops/s` | `4543952.902 ops/s` | `+63.70%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `2132544.818 ops/s` | `3380371.755 ops/s` | `+58.51%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3190965.598 ops/s` | `4117454.751 ops/s` | `+29.03%` | `better` |
| `segment-index-get-persisted:getHitSync` | `2069116.248 ops/s` | `3358859.796 ops/s` | `+62.33%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2862668.178 ops/s` | `4466231.670 ops/s` | `+56.02%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2819452.298 ops/s` | `4269996.134 ops/s` | `+51.45%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1555671.061 ops/s` | `2043622.675 ops/s` | `+31.37%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `119.518 ms/op` | `227.242 ms/op` | `+90.13%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `135.755 ms/op` | `243.252 ms/op` | `+79.18%` | `better` |
| `segment-index-lifecycle:openExisting` | `115.181 ms/op` | `222.846 ms/op` | `+93.47%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `456522.099 ops/s` | `557854.607 ops/s` | `+22.20%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `198821.675 ops/s` | `262415.405 ops/s` | `+31.99%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `257700.424 ops/s` | `295439.202 ops/s` | `+14.64%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `845986.009 ops/s` | `1181841.660 ops/s` | `+39.70%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `808798.112 ops/s` | `1146977.781 ops/s` | `+41.81%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `37187.897 ops/s` | `34863.879 ops/s` | `-6.25%` | `warning` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `8512.516 ops/s` | `8283.575 ops/s` | `-2.69%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:putSync` | `8663.952 ops/s` | `8018.906 ops/s` | `-7.45%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `3784.070 ops/s` | `3651.460 ops/s` | `-3.50%` | `warning` |
| `segment-index-persisted-mutation:putSync` | `3593.520 ops/s` | `3628.877 ops/s` | `+0.98%` | `neutral` |
| `segment-index-range-scan:boundedScan` | `27.280 us/op` | `29.462 us/op` | `+8.00%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2025.669 us/op` | `2035.194 us/op` | `+0.47%` | `neutral` |
| `segment-index-range-scan:sequentialRead` | `3970.682 us/op` | `3931.545 us/op` | `-0.99%` | `neutral` |
| `segment-merge-sequential:mergeSequential` | `336.152 us/op` | `336.006 us/op` | `-0.04%` | `neutral` |

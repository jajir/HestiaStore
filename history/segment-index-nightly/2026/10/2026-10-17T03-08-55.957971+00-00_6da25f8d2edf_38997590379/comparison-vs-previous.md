# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2869795.855 ops/s` | `4886642.297 ops/s` | `+70.28%` | `better` |
| `segment-index-get-live:getMissSync` | `3130468.559 ops/s` | `4461053.131 ops/s` | `+42.50%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `271138.170 ops/s` | `267117.745 ops/s` | `-1.48%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `3129298.828 ops/s` | `4560669.699 ops/s` | `+45.74%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `2376039.137 ops/s` | `3761514.882 ops/s` | `+58.31%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3046569.271 ops/s` | `4622952.189 ops/s` | `+51.74%` | `better` |
| `segment-index-get-persisted:getHitSync` | `2292089.506 ops/s` | `3522015.396 ops/s` | `+53.66%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3236654.809 ops/s` | `4230873.781 ops/s` | `+30.72%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2771753.050 ops/s` | `4022091.106 ops/s` | `+45.11%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1572922.225 ops/s` | `2368423.166 ops/s` | `+50.57%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `134.346 ms/op` | `243.082 ms/op` | `+80.94%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `154.359 ms/op` | `260.770 ms/op` | `+68.94%` | `better` |
| `segment-index-lifecycle:openExisting` | `132.343 ms/op` | `240.347 ms/op` | `+81.61%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `460192.976 ops/s` | `571360.708 ops/s` | `+24.16%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `205250.181 ops/s` | `279224.133 ops/s` | `+36.04%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `254942.795 ops/s` | `292136.576 ops/s` | `+14.59%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `888081.240 ops/s` | `1199189.298 ops/s` | `+35.03%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `851264.785 ops/s` | `1164066.701 ops/s` | `+36.75%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `36816.455 ops/s` | `35122.597 ops/s` | `-4.60%` | `warning` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `9815.182 ops/s` | `8516.268 ops/s` | `-13.23%` | `worse` |
| `segment-index-persisted-mutation-concurrent:putSync` | `9902.948 ops/s` | `8683.454 ops/s` | `-12.31%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `3948.412 ops/s` | `3157.252 ops/s` | `-20.04%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `3714.734 ops/s` | `3080.169 ops/s` | `-17.08%` | `worse` |
| `segment-index-range-scan:boundedScan` | `28.086 us/op` | `29.073 us/op` | `+3.52%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2024.233 us/op` | `1987.441 us/op` | `-1.82%` | `neutral` |
| `segment-index-range-scan:sequentialRead` | `3915.174 us/op` | `3831.888 us/op` | `-2.13%` | `neutral` |
| `segment-merge-sequential:mergeSequential` | `333.194 us/op` | `332.594 us/op` | `-0.18%` | `neutral` |

# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4349310.467 ops/s` | `5004691.386 ops/s` | `+15.07%` | `better` |
| `segment-index-get-live:getMissSync` | `4273517.674 ops/s` | `4743860.567 ops/s` | `+11.01%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `272950.999 ops/s` | `277950.145 ops/s` | `+1.83%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `4225052.818 ops/s` | `4720233.967 ops/s` | `+11.72%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `3368143.650 ops/s` | `3745333.867 ops/s` | `+11.20%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3997461.515 ops/s` | `5600982.082 ops/s` | `+40.11%` | `better` |
| `segment-index-get-persisted:getHitSync` | `3385325.406 ops/s` | `3522901.736 ops/s` | `+4.06%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4522793.237 ops/s` | `5082734.243 ops/s` | `+12.38%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `4061343.140 ops/s` | `4552427.792 ops/s` | `+12.09%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2077289.555 ops/s` | `2394172.601 ops/s` | `+15.25%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `275.022 ms/op` | `245.080 ms/op` | `-10.89%` | `worse` |
| `segment-index-lifecycle:openAndCompact` | `294.256 ms/op` | `262.230 ms/op` | `-10.88%` | `worse` |
| `segment-index-lifecycle:openExisting` | `275.737 ms/op` | `240.296 ms/op` | `-12.85%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `550362.704 ops/s` | `557358.912 ops/s` | `+1.27%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `258202.217 ops/s` | `264693.487 ops/s` | `+2.51%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `292160.487 ops/s` | `292665.425 ops/s` | `+0.17%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1261013.298 ops/s` | `1243703.263 ops/s` | `-1.37%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1227051.028 ops/s` | `1208755.142 ops/s` | `-1.49%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `33962.271 ops/s` | `34948.121 ops/s` | `+2.90%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `7008.463 ops/s` | `8749.071 ops/s` | `+24.84%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `7124.075 ops/s` | `8534.897 ops/s` | `+19.80%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `3073.413 ops/s` | `3158.270 ops/s` | `+2.76%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `3150.008 ops/s` | `3108.922 ops/s` | `-1.30%` | `neutral` |
| `segment-index-range-scan:boundedScan` | `30.839 us/op` | `28.522 us/op` | `-7.51%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2156.720 us/op` | `1972.901 us/op` | `-8.52%` | `worse` |
| `segment-index-range-scan:sequentialRead` | `4157.628 us/op` | `3827.985 us/op` | `-7.93%` | `worse` |
| `segment-merge-sequential:mergeSequential` | `358.818 us/op` | `331.085 us/op` | `-7.73%` | `worse` |

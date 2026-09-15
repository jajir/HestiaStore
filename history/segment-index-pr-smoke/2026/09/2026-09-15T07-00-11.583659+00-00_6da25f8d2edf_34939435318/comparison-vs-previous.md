# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `6ba0871792734698e0f788c02cc5eadf23ed5d65`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `6138812.079 ops/s` | `5189227.345 ops/s` | `-15.47%` | `worse` |
| `segment-index-get-live:getMissSync` | `5438153.696 ops/s` | `4422838.544 ops/s` | `-18.67%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `4601841.063 ops/s` | `3690480.861 ops/s` | `-19.80%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `5792453.698 ops/s` | `4580017.087 ops/s` | `-20.93%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `4402925.386 ops/s` | `3685159.992 ops/s` | `-16.30%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `5444262.665 ops/s` | `4621157.561 ops/s` | `-15.12%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `4852764.631 ops/s` | `4223686.844 ops/s` | `-12.96%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2717586.719 ops/s` | `2151309.060 ops/s` | `-20.84%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `700565.718 ops/s` | `579335.461 ops/s` | `-17.30%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `451945.531 ops/s` | `380940.167 ops/s` | `-15.71%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `248620.187 ops/s` | `198395.294 ops/s` | `-20.20%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1194074.646 ops/s` | `745228.531 ops/s` | `-37.59%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1170442.217 ops/s` | `715459.554 ops/s` | `-38.87%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `23632.429 ops/s` | `29768.977 ops/s` | `+25.97%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `5492.729 ops/s` | `6995.133 ops/s` | `+27.35%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `6225.810 ops/s` | `7033.459 ops/s` | `+12.97%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2942.246 ops/s` | `2833.508 ops/s` | `-3.70%` | `warning` |
| `segment-index-persisted-mutation:putSync` | `2819.952 ops/s` | `2817.656 ops/s` | `-0.08%` | `neutral` |
| `segment-index-range-scan:boundedScan` | `23.534 us/op` | `33.462 us/op` | `+42.19%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1646.850 us/op` | `2078.638 us/op` | `+26.22%` | `better` |
| `segment-index-range-scan:sequentialRead` | `3105.303 us/op` | `4091.306 us/op` | `+31.75%` | `better` |
| `segment-merge-sequential:mergeSequential` | `273.236 us/op` | `324.009 us/op` | `+18.58%` | `better` |

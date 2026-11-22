# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4581738.007 ops/s` | `2818201.496 ops/s` | `-38.49%` | `worse` |
| `segment-index-get-live:getMissSync` | `4302259.672 ops/s` | `2508747.104 ops/s` | `-41.69%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `279648.980 ops/s` | `293646.267 ops/s` | `+5.01%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `4807860.971 ops/s` | `2652049.086 ops/s` | `-44.84%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `3377848.379 ops/s` | `1631792.501 ops/s` | `-51.69%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `4332331.463 ops/s` | `2621302.522 ops/s` | `-39.49%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `3212520.252 ops/s` | `1918539.490 ops/s` | `-40.28%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `4505444.064 ops/s` | `2846990.546 ops/s` | `-36.81%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `3860201.473 ops/s` | `2324536.004 ops/s` | `-39.78%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2126613.869 ops/s` | `1271114.732 ops/s` | `-40.23%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `234.275 ms/op` | `123.364 ms/op` | `-47.34%` | `worse` |
| `segment-index-lifecycle:openAndCompact` | `254.264 ms/op` | `139.045 ms/op` | `-45.31%` | `worse` |
| `segment-index-lifecycle:openExisting` | `231.264 ms/op` | `119.914 ms/op` | `-48.15%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `568876.967 ops/s` | `475662.350 ops/s` | `-16.39%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `276139.141 ops/s` | `214565.873 ops/s` | `-22.30%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `292737.825 ops/s` | `261096.478 ops/s` | `-10.81%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1232094.016 ops/s` | `845524.105 ops/s` | `-31.38%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1198436.293 ops/s` | `804543.375 ops/s` | `-32.87%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `33657.724 ops/s` | `40980.730 ops/s` | `+21.76%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `7373.181 ops/s` | `5863.162 ops/s` | `-20.48%` | `worse` |
| `segment-index-persisted-mutation-concurrent:putSync` | `7444.552 ops/s` | `5839.257 ops/s` | `-21.56%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `3245.555 ops/s` | `2621.924 ops/s` | `-19.21%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `3109.952 ops/s` | `2601.806 ops/s` | `-16.34%` | `worse` |
| `segment-index-range-scan:boundedScan` | `30.801 us/op` | `24.292 us/op` | `-21.13%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2052.559 us/op` | `1827.042 us/op` | `-10.99%` | `worse` |
| `segment-index-range-scan:sequentialRead` | `3853.580 us/op` | `3526.502 us/op` | `-8.49%` | `worse` |
| `segment-merge-sequential:mergeSequential` | `332.872 us/op` | `311.295 us/op` | `-6.48%` | `warning` |

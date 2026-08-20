# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `0944e8c20918f769044f88a7c62655cf623defed`
- Candidate SHA: `6ba0871792734698e0f788c02cc5eadf23ed5d65`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `6076580.097 ops/s` | `6138812.079 ops/s` | `+1.02%` | `neutral` |
| `segment-index-get-live:getMissSync` | `5843703.007 ops/s` | `5438153.696 ops/s` | `-6.94%` | `warning` |
| `segment-index-get-multisegment-hot:getHitSync` | `4556868.535 ops/s` | `4601841.063 ops/s` | `+0.99%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `5458178.632 ops/s` | `5792453.698 ops/s` | `+6.12%` | `better` |
| `segment-index-get-persisted:getHitSync` | `4317460.049 ops/s` | `4402925.386 ops/s` | `+1.98%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `5920451.977 ops/s` | `5444262.665 ops/s` | `-8.04%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `4941760.155 ops/s` | `4852764.631 ops/s` | `-1.80%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2792947.094 ops/s` | `2717586.719 ops/s` | `-2.70%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `657540.717 ops/s` | `700565.718 ops/s` | `+6.54%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `404359.867 ops/s` | `451945.531 ops/s` | `+11.77%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `253180.850 ops/s` | `248620.187 ops/s` | `-1.80%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1482927.237 ops/s` | `1194074.646 ops/s` | `-19.48%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1458310.424 ops/s` | `1170442.217 ops/s` | `-19.74%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `24616.813 ops/s` | `23632.429 ops/s` | `-4.00%` | `warning` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `3462.535 ops/s` | `5492.729 ops/s` | `+58.63%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `3523.490 ops/s` | `6225.810 ops/s` | `+76.69%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `1994.828 ops/s` | `2942.246 ops/s` | `+47.49%` | `better` |
| `segment-index-persisted-mutation:putSync` | `1942.867 ops/s` | `2819.952 ops/s` | `+45.14%` | `better` |
| `segment-index-range-scan:boundedScan` | `23.251 us/op` | `23.534 us/op` | `+1.22%` | `neutral` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1626.288 us/op` | `1646.850 us/op` | `+1.26%` | `neutral` |
| `segment-index-range-scan:sequentialRead` | `3180.528 us/op` | `3105.303 us/op` | `-2.37%` | `neutral` |
| `segment-merge-sequential:mergeSequential` | `273.537 us/op` | `273.236 us/op` | `-0.11%` | `neutral` |

# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6ba0871792734698e0f788c02cc5eadf23ed5d65`
- Candidate SHA: `6ba0871792734698e0f788c02cc5eadf23ed5d65`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `5212664.973 ops/s` | `2816278.402 ops/s` | `-45.97%` | `worse` |
| `segment-index-get-live:getMissSync` | `4579751.284 ops/s` | `2548892.822 ops/s` | `-44.34%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `273969.747 ops/s` | `310227.302 ops/s` | `+13.23%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `4441262.091 ops/s` | `2462734.217 ops/s` | `-44.55%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `3610869.443 ops/s` | `2085831.445 ops/s` | `-42.23%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `4011685.205 ops/s` | `2753604.391 ops/s` | `-31.36%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `3642368.526 ops/s` | `1881974.638 ops/s` | `-48.33%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `4375237.968 ops/s` | `2282619.249 ops/s` | `-47.83%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `4369900.391 ops/s` | `2384342.520 ops/s` | `-45.44%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2270937.419 ops/s` | `1427204.858 ops/s` | `-37.15%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `244.757 ms/op` | `135.799 ms/op` | `-44.52%` | `worse` |
| `segment-index-lifecycle:openAndCompact` | `268.280 ms/op` | `157.064 ms/op` | `-41.46%` | `worse` |
| `segment-index-lifecycle:openExisting` | `243.825 ms/op` | `133.461 ms/op` | `-45.26%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `525134.359 ops/s` | `493067.237 ops/s` | `-6.11%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `253684.369 ops/s` | `218469.467 ops/s` | `-13.88%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `271449.990 ops/s` | `274597.770 ops/s` | `+1.16%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1264818.010 ops/s` | `1117502.824 ops/s` | `-11.65%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1244251.951 ops/s` | `1094048.997 ops/s` | `-12.07%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `20566.059 ops/s` | `23453.827 ops/s` | `+14.04%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `6605.569 ops/s` | `4982.282 ops/s` | `-24.57%` | `worse` |
| `segment-index-persisted-mutation-concurrent:putSync` | `6791.968 ops/s` | `5130.264 ops/s` | `-24.47%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `2326.839 ops/s` | `2113.115 ops/s` | `-9.19%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `2187.610 ops/s` | `2068.349 ops/s` | `-5.45%` | `warning` |
| `segment-index-range-scan:boundedScan` | `27.455 us/op` | `21.956 us/op` | `-20.03%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2023.157 us/op` | `1795.635 us/op` | `-11.25%` | `worse` |
| `segment-index-range-scan:sequentialRead` | `4083.088 us/op` | `3457.454 us/op` | `-15.32%` | `worse` |
| `segment-merge-sequential:mergeSequential` | `332.259 us/op` | `305.859 us/op` | `-7.95%` | `worse` |

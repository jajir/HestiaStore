# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6ba0871792734698e0f788c02cc5eadf23ed5d65`
- Candidate SHA: `6ba0871792734698e0f788c02cc5eadf23ed5d65`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2816278.402 ops/s` | `5513432.857 ops/s` | `+95.77%` | `better` |
| `segment-index-get-live:getMissSync` | `2548892.822 ops/s` | `4685194.498 ops/s` | `+83.81%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `310227.302 ops/s` | `274829.114 ops/s` | `-11.41%` | `worse` |
| `segment-index-get-multisegment-cold:getMissSync` | `2462734.217 ops/s` | `4711194.468 ops/s` | `+91.30%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `2085831.445 ops/s` | `3498813.754 ops/s` | `+67.74%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `2753604.391 ops/s` | `4626266.878 ops/s` | `+68.01%` | `better` |
| `segment-index-get-persisted:getHitSync` | `1881974.638 ops/s` | `3227533.288 ops/s` | `+71.50%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2282619.249 ops/s` | `4578148.838 ops/s` | `+100.57%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2384342.520 ops/s` | `4428074.065 ops/s` | `+85.71%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1427204.858 ops/s` | `2016695.549 ops/s` | `+41.30%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `135.799 ms/op` | `245.321 ms/op` | `+80.65%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `157.064 ms/op` | `266.589 ms/op` | `+69.73%` | `better` |
| `segment-index-lifecycle:openExisting` | `133.461 ms/op` | `241.067 ms/op` | `+80.63%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `493067.237 ops/s` | `524841.156 ops/s` | `+6.44%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `218469.467 ops/s` | `252917.351 ops/s` | `+15.77%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `274597.770 ops/s` | `271923.805 ops/s` | `-0.97%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1117502.824 ops/s` | `1178468.972 ops/s` | `+5.46%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1094048.997 ops/s` | `1157170.575 ops/s` | `+5.77%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `23453.827 ops/s` | `21298.397 ops/s` | `-9.19%` | `worse` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `4982.282 ops/s` | `7439.368 ops/s` | `+49.32%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `5130.264 ops/s` | `7235.123 ops/s` | `+41.03%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2113.115 ops/s` | `2486.467 ops/s` | `+17.67%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2068.349 ops/s` | `2450.681 ops/s` | `+18.48%` | `better` |
| `segment-index-range-scan:boundedScan` | `21.956 us/op` | `29.856 us/op` | `+35.98%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1795.635 us/op` | `2041.924 us/op` | `+13.72%` | `better` |
| `segment-index-range-scan:sequentialRead` | `3457.454 us/op` | `3907.542 us/op` | `+13.02%` | `better` |
| `segment-merge-sequential:mergeSequential` | `305.859 us/op` | `331.899 us/op` | `+8.51%` | `better` |

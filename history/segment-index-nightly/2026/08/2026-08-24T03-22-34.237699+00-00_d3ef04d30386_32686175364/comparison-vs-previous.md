# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6ba0871792734698e0f788c02cc5eadf23ed5d65`
- Candidate SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `5513432.857 ops/s` | `4786596.752 ops/s` | `-13.18%` | `worse` |
| `segment-index-get-live:getMissSync` | `4685194.498 ops/s` | `4697862.682 ops/s` | `+0.27%` | `neutral` |
| `segment-index-get-multisegment-cold:getHitSync` | `274829.114 ops/s` | `284983.426 ops/s` | `+3.69%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `4711194.468 ops/s` | `4498541.649 ops/s` | `-4.51%` | `warning` |
| `segment-index-get-multisegment-hot:getHitSync` | `3498813.754 ops/s` | `3522470.290 ops/s` | `+0.68%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `4626266.878 ops/s` | `4410630.665 ops/s` | `-4.66%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `3227533.288 ops/s` | `3654607.694 ops/s` | `+13.23%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4578148.838 ops/s` | `4579523.706 ops/s` | `+0.03%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `4428074.065 ops/s` | `4016096.915 ops/s` | `-9.30%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2016695.549 ops/s` | `2256071.798 ops/s` | `+11.87%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `245.321 ms/op` | `279.373 ms/op` | `+13.88%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `266.589 ms/op` | `299.841 ms/op` | `+12.47%` | `better` |
| `segment-index-lifecycle:openExisting` | `241.067 ms/op` | `273.049 ms/op` | `+13.27%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `524841.156 ops/s` | `544330.661 ops/s` | `+3.71%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `252917.351 ops/s` | `259458.994 ops/s` | `+2.59%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `271923.805 ops/s` | `284871.667 ops/s` | `+4.76%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1178468.972 ops/s` | `1228049.722 ops/s` | `+4.21%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1157170.575 ops/s` | `1206958.447 ops/s` | `+4.30%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `21298.397 ops/s` | `21091.275 ops/s` | `-0.97%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `7439.368 ops/s` | `7664.545 ops/s` | `+3.03%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `7235.123 ops/s` | `7798.317 ops/s` | `+7.78%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2486.467 ops/s` | `3119.943 ops/s` | `+25.48%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2450.681 ops/s` | `3024.272 ops/s` | `+23.41%` | `better` |
| `segment-index-range-scan:boundedScan` | `29.856 us/op` | `31.793 us/op` | `+6.49%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2041.924 us/op` | `2100.025 us/op` | `+2.85%` | `neutral` |
| `segment-index-range-scan:sequentialRead` | `3907.542 us/op` | `4077.554 us/op` | `+4.35%` | `better` |
| `segment-merge-sequential:mergeSequential` | `331.899 us/op` | `357.941 us/op` | `+7.85%` | `better` |

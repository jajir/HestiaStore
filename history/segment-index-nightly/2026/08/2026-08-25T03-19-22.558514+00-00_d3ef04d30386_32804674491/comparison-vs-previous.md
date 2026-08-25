# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Candidate SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4786596.752 ops/s` | `2936211.793 ops/s` | `-38.66%` | `worse` |
| `segment-index-get-live:getMissSync` | `4697862.682 ops/s` | `2525102.012 ops/s` | `-46.25%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `284983.426 ops/s` | `380770.221 ops/s` | `+33.61%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `4498541.649 ops/s` | `3016051.460 ops/s` | `-32.95%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `3522470.290 ops/s` | `2445659.943 ops/s` | `-30.57%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `4410630.665 ops/s` | `2669505.729 ops/s` | `-39.48%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `3654607.694 ops/s` | `2550997.271 ops/s` | `-30.20%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `4579523.706 ops/s` | `2893036.087 ops/s` | `-36.83%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `4016096.915 ops/s` | `2639072.958 ops/s` | `-34.29%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2256071.798 ops/s` | `1401533.611 ops/s` | `-37.88%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `279.373 ms/op` | `99.504 ms/op` | `-64.38%` | `worse` |
| `segment-index-lifecycle:openAndCompact` | `299.841 ms/op` | `109.457 ms/op` | `-63.49%` | `worse` |
| `segment-index-lifecycle:openExisting` | `273.049 ms/op` | `98.216 ms/op` | `-64.03%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `544330.661 ops/s` | `615988.152 ops/s` | `+13.16%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `259458.994 ops/s` | `315827.735 ops/s` | `+21.73%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `284871.667 ops/s` | `300160.416 ops/s` | `+5.37%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1228049.722 ops/s` | `1485674.916 ops/s` | `+20.98%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1206958.447 ops/s` | `1457498.294 ops/s` | `+20.76%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `21091.275 ops/s` | `28176.622 ops/s` | `+33.59%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `7664.545 ops/s` | `828.459 ops/s` | `-89.19%` | `worse` |
| `segment-index-persisted-mutation-concurrent:putSync` | `7798.317 ops/s` | `729.801 ops/s` | `-90.64%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `3119.943 ops/s` | `332.128 ops/s` | `-89.35%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `3024.272 ops/s` | `875.992 ops/s` | `-71.03%` | `worse` |
| `segment-index-range-scan:boundedScan` | `31.793 us/op` | `18.330 us/op` | `-42.35%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2100.025 us/op` | `1447.502 us/op` | `-31.07%` | `worse` |
| `segment-index-range-scan:sequentialRead` | `4077.554 us/op` | `2791.527 us/op` | `-31.54%` | `worse` |
| `segment-merge-sequential:mergeSequential` | `357.941 us/op` | `220.702 us/op` | `-38.34%` | `worse` |

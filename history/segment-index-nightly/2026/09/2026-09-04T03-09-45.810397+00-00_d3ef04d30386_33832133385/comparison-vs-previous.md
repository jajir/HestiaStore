# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Candidate SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `5597360.863 ops/s` | `4476241.859 ops/s` | `-20.03%` | `worse` |
| `segment-index-get-live:getMissSync` | `6077174.373 ops/s` | `4753537.048 ops/s` | `-21.78%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `376937.470 ops/s` | `270753.486 ops/s` | `-28.17%` | `worse` |
| `segment-index-get-multisegment-cold:getMissSync` | `5986058.104 ops/s` | `4666913.326 ops/s` | `-22.04%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `4694212.529 ops/s` | `3496801.078 ops/s` | `-25.51%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `5456744.981 ops/s` | `3798067.794 ops/s` | `-30.40%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `4562645.512 ops/s` | `3627990.776 ops/s` | `-20.48%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `5134551.475 ops/s` | `4238870.652 ops/s` | `-17.44%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `5262809.837 ops/s` | `4011258.669 ops/s` | `-23.78%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2741218.013 ops/s` | `1875489.796 ops/s` | `-31.58%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `214.595 ms/op` | `278.767 ms/op` | `+29.90%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `232.548 ms/op` | `301.502 ms/op` | `+29.65%` | `better` |
| `segment-index-lifecycle:openExisting` | `212.609 ms/op` | `273.883 ms/op` | `+28.82%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `666026.759 ops/s` | `534493.516 ops/s` | `-19.75%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `354934.508 ops/s` | `257143.570 ops/s` | `-27.55%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `311092.251 ops/s` | `277349.946 ops/s` | `-10.85%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1758638.338 ops/s` | `1203289.457 ops/s` | `-31.58%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1735321.964 ops/s` | `1183737.738 ops/s` | `-31.79%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `23316.374 ops/s` | `19551.718 ops/s` | `-16.15%` | `worse` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `3760.663 ops/s` | `7374.336 ops/s` | `+96.09%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `3990.751 ops/s` | `7635.497 ops/s` | `+91.33%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2249.316 ops/s` | `3104.235 ops/s` | `+38.01%` | `better` |
| `segment-index-persisted-mutation:putSync` | `1496.929 ops/s` | `2935.007 ops/s` | `+96.07%` | `better` |
| `segment-index-range-scan:boundedScan` | `22.851 us/op` | `29.157 us/op` | `+27.60%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1581.909 us/op` | `2137.894 us/op` | `+35.15%` | `better` |
| `segment-index-range-scan:sequentialRead` | `3032.337 us/op` | `4175.662 us/op` | `+37.70%` | `better` |
| `segment-merge-sequential:mergeSequential` | `279.447 us/op` | `358.607 us/op` | `+28.33%` | `better` |

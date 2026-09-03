# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Candidate SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4999366.306 ops/s` | `5597360.863 ops/s` | `+11.96%` | `better` |
| `segment-index-get-live:getMissSync` | `4685847.911 ops/s` | `6077174.373 ops/s` | `+29.69%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `269310.898 ops/s` | `376937.470 ops/s` | `+39.96%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `4473987.817 ops/s` | `5986058.104 ops/s` | `+33.80%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `3320817.560 ops/s` | `4694212.529 ops/s` | `+41.36%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `4365724.705 ops/s` | `5456744.981 ops/s` | `+24.99%` | `better` |
| `segment-index-get-persisted:getHitSync` | `3374183.807 ops/s` | `4562645.512 ops/s` | `+35.22%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4598536.349 ops/s` | `5134551.475 ops/s` | `+11.66%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `4098935.396 ops/s` | `5262809.837 ops/s` | `+28.39%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2092869.215 ops/s` | `2741218.013 ops/s` | `+30.98%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `276.949 ms/op` | `214.595 ms/op` | `-22.51%` | `worse` |
| `segment-index-lifecycle:openAndCompact` | `299.096 ms/op` | `232.548 ms/op` | `-22.25%` | `worse` |
| `segment-index-lifecycle:openExisting` | `276.153 ms/op` | `212.609 ms/op` | `-23.01%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `566686.151 ops/s` | `666026.759 ops/s` | `+17.53%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `292374.218 ops/s` | `354934.508 ops/s` | `+21.40%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `274311.934 ops/s` | `311092.251 ops/s` | `+13.41%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1311005.669 ops/s` | `1758638.338 ops/s` | `+34.14%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1290810.693 ops/s` | `1735321.964 ops/s` | `+34.44%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `20194.977 ops/s` | `23316.374 ops/s` | `+15.46%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `7739.255 ops/s` | `3760.663 ops/s` | `-51.41%` | `worse` |
| `segment-index-persisted-mutation-concurrent:putSync` | `7762.686 ops/s` | `3990.751 ops/s` | `-48.59%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `3091.096 ops/s` | `2249.316 ops/s` | `-27.23%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `3027.883 ops/s` | `1496.929 ops/s` | `-50.56%` | `worse` |
| `segment-index-range-scan:boundedScan` | `31.146 us/op` | `22.851 us/op` | `-26.63%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2111.974 us/op` | `1581.909 us/op` | `-25.10%` | `worse` |
| `segment-index-range-scan:sequentialRead` | `4245.295 us/op` | `3032.337 us/op` | `-28.57%` | `worse` |
| `segment-merge-sequential:mergeSequential` | `358.379 us/op` | `279.447 us/op` | `-22.02%` | `worse` |

# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `8001f8252a3e24c28f05deafffa0fc6b4ef3247a`
- Candidate SHA: `99f6033423f943fa22655f00429ed039230292dc`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `6240663.082 ops/s` | `5069358.714 ops/s` | `-18.77%` | `worse` |
| `segment-index-get-live:getMissSync` | `5850987.323 ops/s` | `4763404.802 ops/s` | `-18.59%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `4512735.115 ops/s` | `3641062.466 ops/s` | `-19.32%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `5706234.455 ops/s` | `4651683.036 ops/s` | `-18.48%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `4288156.460 ops/s` | `3232080.168 ops/s` | `-24.63%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `6055495.024 ops/s` | `4615968.821 ops/s` | `-23.77%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `5270032.109 ops/s` | `4047011.494 ops/s` | `-23.21%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2745576.244 ops/s` | `2351156.207 ops/s` | `-14.37%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `615150.736 ops/s` | `571150.619 ops/s` | `-7.15%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `384846.685 ops/s` | `410413.579 ops/s` | `+6.64%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `230304.050 ops/s` | `160737.040 ops/s` | `-30.21%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1291019.329 ops/s` | `757169.498 ops/s` | `-41.35%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1275825.433 ops/s` | `743338.041 ops/s` | `-41.74%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `15193.896 ops/s` | `13831.457 ops/s` | `-8.97%` | `worse` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `5363.318 ops/s` | `6417.821 ops/s` | `+19.66%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `5477.726 ops/s` | `6168.558 ops/s` | `+12.61%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2691.927 ops/s` | `2539.748 ops/s` | `-5.65%` | `warning` |
| `segment-index-persisted-mutation:putSync` | `2492.139 ops/s` | `2467.281 ops/s` | `-1.00%` | `neutral` |
| `segment-index-range-scan:boundedScan` | `23.949 us/op` | `29.788 us/op` | `+24.38%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1651.722 us/op` | `2148.160 us/op` | `+30.06%` | `better` |
| `segment-index-range-scan:sequentialRead` | `3167.353 us/op` | `4048.053 us/op` | `+27.81%` | `better` |
| `segment-merge-sequential:mergeSequential` | `278.044 us/op` | `323.399 us/op` | `+16.31%` | `better` |

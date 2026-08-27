# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Candidate SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `5680824.953 ops/s` | `3360891.846 ops/s` | `-40.84%` | `worse` |
| `segment-index-get-live:getMissSync` | `5410111.787 ops/s` | `2424808.183 ops/s` | `-55.18%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `403418.935 ops/s` | `362615.197 ops/s` | `-10.11%` | `worse` |
| `segment-index-get-multisegment-cold:getMissSync` | `5756577.563 ops/s` | `2972700.350 ops/s` | `-48.36%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `4527335.623 ops/s` | `2280032.143 ops/s` | `-49.64%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `5644613.980 ops/s` | `2819728.585 ops/s` | `-50.05%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `4337979.512 ops/s` | `2287533.485 ops/s` | `-47.27%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `5729598.966 ops/s` | `2597259.609 ops/s` | `-54.67%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `5073653.976 ops/s` | `2728826.058 ops/s` | `-46.22%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2664522.562 ops/s` | `1334360.262 ops/s` | `-49.92%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `212.079 ms/op` | `97.468 ms/op` | `-54.04%` | `worse` |
| `segment-index-lifecycle:openAndCompact` | `232.009 ms/op` | `114.759 ms/op` | `-50.54%` | `worse` |
| `segment-index-lifecycle:openExisting` | `213.256 ms/op` | `91.638 ms/op` | `-57.03%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `660446.449 ops/s` | `597828.044 ops/s` | `-9.48%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `347215.291 ops/s` | `300633.353 ops/s` | `-13.42%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `313231.158 ops/s` | `297194.691 ops/s` | `-5.12%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1765642.347 ops/s` | `1298501.825 ops/s` | `-26.46%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1741715.013 ops/s` | `1270822.281 ops/s` | `-27.04%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `23927.334 ops/s` | `27679.544 ops/s` | `+15.68%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `3827.514 ops/s` | `660.479 ops/s` | `-82.74%` | `worse` |
| `segment-index-persisted-mutation-concurrent:putSync` | `4814.039 ops/s` | `579.045 ops/s` | `-87.97%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `2224.433 ops/s` | `109.736 ops/s` | `-95.07%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `1345.591 ops/s` | `354.300 ops/s` | `-73.67%` | `worse` |
| `segment-index-range-scan:boundedScan` | `23.250 us/op` | `20.404 us/op` | `-12.24%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1566.085 us/op` | `1418.534 us/op` | `-9.42%` | `worse` |
| `segment-index-range-scan:sequentialRead` | `2997.187 us/op` | `2721.067 us/op` | `-9.21%` | `worse` |
| `segment-merge-sequential:mergeSequential` | `280.129 us/op` | `218.819 us/op` | `-21.89%` | `worse` |

# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Candidate SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `3360891.846 ops/s` | `4612407.089 ops/s` | `+37.24%` | `better` |
| `segment-index-get-live:getMissSync` | `2424808.183 ops/s` | `4054246.793 ops/s` | `+67.20%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `362615.197 ops/s` | `290257.091 ops/s` | `-19.95%` | `worse` |
| `segment-index-get-multisegment-cold:getMissSync` | `2972700.350 ops/s` | `4479127.484 ops/s` | `+50.68%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `2280032.143 ops/s` | `3652190.985 ops/s` | `+60.18%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `2819728.585 ops/s` | `4683766.082 ops/s` | `+66.11%` | `better` |
| `segment-index-get-persisted:getHitSync` | `2287533.485 ops/s` | `3120462.143 ops/s` | `+36.41%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2597259.609 ops/s` | `4940835.136 ops/s` | `+90.23%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2728826.058 ops/s` | `4371477.717 ops/s` | `+60.20%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1334360.262 ops/s` | `2143143.816 ops/s` | `+60.61%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `97.468 ms/op` | `277.943 ms/op` | `+185.16%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `114.759 ms/op` | `299.553 ms/op` | `+161.03%` | `better` |
| `segment-index-lifecycle:openExisting` | `91.638 ms/op` | `272.378 ms/op` | `+197.23%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `597828.044 ops/s` | `561082.312 ops/s` | `-6.15%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `300633.353 ops/s` | `282728.900 ops/s` | `-5.96%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `297194.691 ops/s` | `278353.412 ops/s` | `-6.34%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1298501.825 ops/s` | `1234286.583 ops/s` | `-4.95%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1270822.281 ops/s` | `1214336.309 ops/s` | `-4.44%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `27679.544 ops/s` | `19950.273 ops/s` | `-27.92%` | `worse` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `660.479 ops/s` | `8394.100 ops/s` | `+1170.91%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `579.045 ops/s` | `8443.532 ops/s` | `+1358.18%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `109.736 ops/s` | `3375.342 ops/s` | `+2975.89%` | `better` |
| `segment-index-persisted-mutation:putSync` | `354.300 ops/s` | `3187.939 ops/s` | `+799.79%` | `better` |
| `segment-index-range-scan:boundedScan` | `20.404 us/op` | `32.177 us/op` | `+57.69%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1418.534 us/op` | `2107.331 us/op` | `+48.56%` | `better` |
| `segment-index-range-scan:sequentialRead` | `2721.067 us/op` | `4053.673 us/op` | `+48.97%` | `better` |
| `segment-merge-sequential:mergeSequential` | `218.819 us/op` | `358.814 us/op` | `+63.98%` | `better` |

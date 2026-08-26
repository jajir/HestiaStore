# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Candidate SHA: `95623654fcf369c327465dfac0a497f16b7214e6`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4707024.508 ops/s` | `6477734.247 ops/s` | `+37.62%` | `better` |
| `segment-index-get-live:getMissSync` | `4495963.676 ops/s` | `5903401.104 ops/s` | `+31.30%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `3505546.177 ops/s` | `4716891.299 ops/s` | `+34.56%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `4467706.587 ops/s` | `5692752.807 ops/s` | `+27.42%` | `better` |
| `segment-index-get-persisted:getHitSync` | `3216912.159 ops/s` | `4279717.204 ops/s` | `+33.04%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4300515.598 ops/s` | `5952653.671 ops/s` | `+38.42%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `3913103.347 ops/s` | `5037513.704 ops/s` | `+28.73%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2027743.124 ops/s` | `2782986.665 ops/s` | `+37.25%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `559248.321 ops/s` | `597711.289 ops/s` | `+6.88%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `378200.966 ops/s` | `352173.040 ops/s` | `-6.88%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `181047.356 ops/s` | `245538.249 ops/s` | `+35.62%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `783679.960 ops/s` | `1401485.118 ops/s` | `+78.83%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `764857.253 ops/s` | `1375586.192 ops/s` | `+79.85%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `18822.706 ops/s` | `25898.925 ops/s` | `+37.59%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `7762.050 ops/s` | `5956.226 ops/s` | `-23.26%` | `worse` |
| `segment-index-persisted-mutation-concurrent:putSync` | `7624.602 ops/s` | `6139.152 ops/s` | `-19.48%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `3573.011 ops/s` | `2783.077 ops/s` | `-22.11%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `3552.623 ops/s` | `2802.594 ops/s` | `-21.11%` | `worse` |
| `segment-index-range-scan:boundedScan` | `34.903 us/op` | `23.058 us/op` | `-33.94%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2141.080 us/op` | `1591.128 us/op` | `-25.69%` | `worse` |
| `segment-index-range-scan:sequentialRead` | `4329.613 us/op` | `3205.368 us/op` | `-25.97%` | `worse` |
| `segment-merge-sequential:mergeSequential` | `349.043 us/op` | `272.376 us/op` | `-21.97%` | `worse` |

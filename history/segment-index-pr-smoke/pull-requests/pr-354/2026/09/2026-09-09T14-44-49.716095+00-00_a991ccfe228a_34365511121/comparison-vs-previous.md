# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Candidate SHA: `a991ccfe228ac96396cc85daecb7de6b09054937`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4707024.508 ops/s` | `2874233.545 ops/s` | `-38.94%` | `worse` |
| `segment-index-get-live:getMissSync` | `4495963.676 ops/s` | `2527003.268 ops/s` | `-43.79%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `3505546.177 ops/s` | `2272014.051 ops/s` | `-35.19%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `4467706.587 ops/s` | `2913472.599 ops/s` | `-34.79%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `3216912.159 ops/s` | `2116285.383 ops/s` | `-34.21%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `4300515.598 ops/s` | `2788234.821 ops/s` | `-35.17%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `3913103.347 ops/s` | `2607800.230 ops/s` | `-33.36%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2027743.124 ops/s` | `1438479.450 ops/s` | `-29.06%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `559248.321 ops/s` | `537604.245 ops/s` | `-3.87%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `378200.966 ops/s` | `314152.175 ops/s` | `-16.94%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `181047.356 ops/s` | `223452.070 ops/s` | `+23.42%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `783679.960 ops/s` | `815193.925 ops/s` | `+4.02%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `764857.253 ops/s` | `792788.334 ops/s` | `+3.65%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `18822.706 ops/s` | `22405.590 ops/s` | `+19.03%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `7762.050 ops/s` | `1554.726 ops/s` | `-79.97%` | `worse` |
| `segment-index-persisted-mutation-concurrent:putSync` | `7624.602 ops/s` | `351.694 ops/s` | `-95.39%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `3573.011 ops/s` | `339.400 ops/s` | `-90.50%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `3552.623 ops/s` | `100.013 ops/s` | `-97.18%` | `worse` |
| `segment-index-range-scan:boundedScan` | `34.903 us/op` | `23.347 us/op` | `-33.11%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2141.080 us/op` | `1812.214 us/op` | `-15.36%` | `worse` |
| `segment-index-range-scan:sequentialRead` | `4329.613 us/op` | `3489.413 us/op` | `-19.41%` | `worse` |
| `segment-merge-sequential:mergeSequential` | `349.043 us/op` | `290.812 us/op` | `-16.68%` | `worse` |

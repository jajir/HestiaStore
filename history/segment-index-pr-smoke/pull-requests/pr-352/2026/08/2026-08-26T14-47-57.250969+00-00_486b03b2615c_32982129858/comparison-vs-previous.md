# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Candidate SHA: `486b03b2615cb411cc1c320d9cc9b36be00b3089`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4707024.508 ops/s` | `4770564.729 ops/s` | `+1.35%` | `neutral` |
| `segment-index-get-live:getMissSync` | `4495963.676 ops/s` | `4690863.180 ops/s` | `+4.33%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `3505546.177 ops/s` | `3586482.835 ops/s` | `+2.31%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `4467706.587 ops/s` | `4511359.382 ops/s` | `+0.98%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `3216912.159 ops/s` | `3246347.981 ops/s` | `+0.92%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `4300515.598 ops/s` | `4794207.587 ops/s` | `+11.48%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `3913103.347 ops/s` | `4217339.003 ops/s` | `+7.77%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2027743.124 ops/s` | `2250281.596 ops/s` | `+10.97%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `559248.321 ops/s` | `585984.289 ops/s` | `+4.78%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `378200.966 ops/s` | `395713.749 ops/s` | `+4.63%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `181047.356 ops/s` | `190270.541 ops/s` | `+5.09%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `783679.960 ops/s` | `682467.720 ops/s` | `-12.91%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `764857.253 ops/s` | `665269.408 ops/s` | `-13.02%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `18822.706 ops/s` | `17198.312 ops/s` | `-8.63%` | `worse` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `7762.050 ops/s` | `6417.457 ops/s` | `-17.32%` | `worse` |
| `segment-index-persisted-mutation-concurrent:putSync` | `7624.602 ops/s` | `6245.575 ops/s` | `-18.09%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `3573.011 ops/s` | `2505.370 ops/s` | `-29.88%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `3552.623 ops/s` | `2521.186 ops/s` | `-29.03%` | `worse` |
| `segment-index-range-scan:boundedScan` | `34.903 us/op` | `27.030 us/op` | `-22.56%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2141.080 us/op` | `2059.870 us/op` | `-3.79%` | `warning` |
| `segment-index-range-scan:sequentialRead` | `4329.613 us/op` | `4020.068 us/op` | `-7.15%` | `worse` |
| `segment-merge-sequential:mergeSequential` | `349.043 us/op` | `324.650 us/op` | `-6.99%` | `warning` |

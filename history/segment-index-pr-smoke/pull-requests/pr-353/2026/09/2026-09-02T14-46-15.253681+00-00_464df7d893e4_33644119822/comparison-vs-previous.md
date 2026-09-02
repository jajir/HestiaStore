# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Candidate SHA: `464df7d893e40e737f8a92cb30c4b0aee0475612`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4707024.508 ops/s` | `4633394.592 ops/s` | `-1.56%` | `neutral` |
| `segment-index-get-live:getMissSync` | `4495963.676 ops/s` | `4487286.382 ops/s` | `-0.19%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `3505546.177 ops/s` | `3493198.556 ops/s` | `-0.35%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `4467706.587 ops/s` | `4287574.135 ops/s` | `-4.03%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `3216912.159 ops/s` | `3427969.836 ops/s` | `+6.56%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4300515.598 ops/s` | `4629278.439 ops/s` | `+7.64%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `3913103.347 ops/s` | `3976298.396 ops/s` | `+1.61%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2027743.124 ops/s` | `1992824.446 ops/s` | `-1.72%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `559248.321 ops/s` | `560842.310 ops/s` | `+0.29%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `378200.966 ops/s` | `362496.983 ops/s` | `-4.15%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `181047.356 ops/s` | `198345.327 ops/s` | `+9.55%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `783679.960 ops/s` | `706916.741 ops/s` | `-9.80%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `764857.253 ops/s` | `687809.668 ops/s` | `-10.07%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `18822.706 ops/s` | `19107.073 ops/s` | `+1.51%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `7762.050 ops/s` | `7486.303 ops/s` | `-3.55%` | `warning` |
| `segment-index-persisted-mutation-concurrent:putSync` | `7624.602 ops/s` | `7165.050 ops/s` | `-6.03%` | `warning` |
| `segment-index-persisted-mutation:deleteSync` | `3573.011 ops/s` | `3487.825 ops/s` | `-2.38%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `3552.623 ops/s` | `3348.921 ops/s` | `-5.73%` | `warning` |
| `segment-index-range-scan:boundedScan` | `34.903 us/op` | `27.995 us/op` | `-19.79%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2141.080 us/op` | `2151.715 us/op` | `+0.50%` | `neutral` |
| `segment-index-range-scan:sequentialRead` | `4329.613 us/op` | `4223.645 us/op` | `-2.45%` | `neutral` |
| `segment-merge-sequential:mergeSequential` | `349.043 us/op` | `349.229 us/op` | `+0.05%` | `neutral` |

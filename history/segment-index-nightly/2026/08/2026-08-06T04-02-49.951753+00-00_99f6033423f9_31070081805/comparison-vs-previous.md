# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `99f6033423f943fa22655f00429ed039230292dc`
- Candidate SHA: `99f6033423f943fa22655f00429ed039230292dc`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `5035513.717 ops/s` | `4806440.449 ops/s` | `-4.55%` | `warning` |
| `segment-index-get-live:getMissSync` | `4480301.133 ops/s` | `4910644.966 ops/s` | `+9.61%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `276370.183 ops/s` | `290734.876 ops/s` | `+5.20%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `4501690.648 ops/s` | `4870068.333 ops/s` | `+8.18%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `3358085.276 ops/s` | `3700299.349 ops/s` | `+10.19%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `4168120.901 ops/s` | `4719058.401 ops/s` | `+13.22%` | `better` |
| `segment-index-get-persisted:getHitSync` | `3182150.061 ops/s` | `3526907.895 ops/s` | `+10.83%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4316241.151 ops/s` | `4677489.162 ops/s` | `+8.37%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `3846859.000 ops/s` | `4377921.711 ops/s` | `+13.81%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1947709.018 ops/s` | `2281667.178 ops/s` | `+17.15%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `277.114 ms/op` | `238.569 ms/op` | `-13.91%` | `worse` |
| `segment-index-lifecycle:openAndCompact` | `302.488 ms/op` | `259.898 ms/op` | `-14.08%` | `worse` |
| `segment-index-lifecycle:openExisting` | `273.909 ms/op` | `236.215 ms/op` | `-13.76%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `509139.484 ops/s` | `546735.015 ops/s` | `+7.38%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `261870.262 ops/s` | `296328.295 ops/s` | `+13.16%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `247269.221 ops/s` | `250406.720 ops/s` | `+1.27%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1222512.600 ops/s` | `1173482.059 ops/s` | `-4.01%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1207880.485 ops/s` | `1159173.123 ops/s` | `-4.03%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `14632.116 ops/s` | `14308.936 ops/s` | `-2.21%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `7359.222 ops/s` | `7505.288 ops/s` | `+1.98%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:putSync` | `7618.728 ops/s` | `7717.680 ops/s` | `+1.30%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2858.586 ops/s` | `2583.589 ops/s` | `-9.62%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `2777.691 ops/s` | `2478.597 ops/s` | `-10.77%` | `worse` |
| `segment-index-range-scan:boundedScan` | `35.998 us/op` | `30.156 us/op` | `-16.23%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2153.581 us/op` | `1986.823 us/op` | `-7.74%` | `worse` |
| `segment-index-range-scan:sequentialRead` | `4209.077 us/op` | `3898.834 us/op` | `-7.37%` | `worse` |
| `segment-merge-sequential:mergeSequential` | `366.638 us/op` | `327.961 us/op` | `-10.55%` | `worse` |

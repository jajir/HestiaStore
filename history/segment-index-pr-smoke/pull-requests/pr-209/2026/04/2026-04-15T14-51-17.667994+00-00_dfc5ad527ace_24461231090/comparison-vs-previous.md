# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `30f93a9be86df7361bbeb5cf93c3547cbb155d00`
- Candidate SHA: `dfc5ad527acec1930f8dfe275f3c458680a228b8`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `3749813.398 ops/s` | `3584265.071 ops/s` | `-4.41%` | `warning` |
| `segment-index-get-live:getMissSync` | `4189285.737 ops/s` | `4035028.840 ops/s` | `-3.68%` | `warning` |
| `segment-index-get-multisegment-hot:getHitSync` | `189.306 ops/s` | `87.083 ops/s` | `-54.00%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `3784895.365 ops/s` | `4059766.798 ops/s` | `+7.26%` | `better` |
| `segment-index-get-persisted:getHitSync` | `110444.454 ops/s` | `113406.905 ops/s` | `+2.68%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `4170827.039 ops/s` | `4033682.393 ops/s` | `-3.29%` | `warning` |
| `segment-index-hot-route-put:putHotRoute` | `2448285.571 ops/s` | `2394934.245 ops/s` | `-2.18%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1511518.429 ops/s` | `1454802.487 ops/s` | `-3.75%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `378439.304 ops/s` | `317837.652 ops/s` | `-16.01%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `225864.679 ops/s` | `153441.798 ops/s` | `-32.06%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `152574.625 ops/s` | `164395.854 ops/s` | `+7.75%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `38200.745 ops/s` | `41127.946 ops/s` | `+7.66%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `29390.941 ops/s` | `33893.248 ops/s` | `+15.32%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `8809.804 ops/s` | `7234.698 ops/s` | `-17.88%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `2393.778 ops/s` | `2430.014 ops/s` | `+1.51%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `2375.777 ops/s` | `2327.884 ops/s` | `-2.02%` | `neutral` |

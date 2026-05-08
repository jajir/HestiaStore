# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `c6fc219651a1ec4c3f5077c33869c54b9af0720e`
- Candidate SHA: `438d4c606becfb314b0d1867aa63f4abdba30751`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2196926.250 ops/s` | `2327072.768 ops/s` | `+5.92%` | `better` |
| `segment-index-get-live:getMissSync` | `3733048.434 ops/s` | `3923572.163 ops/s` | `+5.10%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `7110.114 ops/s` | `7053.646 ops/s` | `-0.79%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3392512.059 ops/s` | `3644912.954 ops/s` | `+7.44%` | `better` |
| `segment-index-get-persisted:getHitSync` | `114582.584 ops/s` | `122761.108 ops/s` | `+7.14%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3585836.650 ops/s` | `4026586.080 ops/s` | `+12.29%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2071669.671 ops/s` | `1928685.071 ops/s` | `-6.90%` | `warning` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1097470.639 ops/s` | `1042717.687 ops/s` | `-4.99%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `295360.431 ops/s` | `309122.580 ops/s` | `+4.66%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `124881.577 ops/s` | `139000.099 ops/s` | `+11.31%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `170478.854 ops/s` | `170122.480 ops/s` | `-0.21%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `46321.849 ops/s` | `40761.728 ops/s` | `-12.00%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `41029.747 ops/s` | `35508.101 ops/s` | `-13.46%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `5292.102 ops/s` | `5253.628 ops/s` | `-0.73%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `3650.550 ops/s` | `2704.476 ops/s` | `-25.92%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `462.338 ops/s` | `449.300 ops/s` | `-2.82%` | `neutral` |

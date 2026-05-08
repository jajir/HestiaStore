# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `c6fc219651a1ec4c3f5077c33869c54b9af0720e`
- Candidate SHA: `747eb62b4b969daa2db907f5181821eb713924b4`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2196926.250 ops/s` | `2185966.983 ops/s` | `-0.50%` | `neutral` |
| `segment-index-get-live:getMissSync` | `3733048.434 ops/s` | `4074591.560 ops/s` | `+9.15%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `7110.114 ops/s` | `6962.273 ops/s` | `-2.08%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3392512.059 ops/s` | `3737196.130 ops/s` | `+10.16%` | `better` |
| `segment-index-get-persisted:getHitSync` | `114582.584 ops/s` | `112580.178 ops/s` | `-1.75%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `3585836.650 ops/s` | `4144663.394 ops/s` | `+15.58%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2071669.671 ops/s` | `2157374.898 ops/s` | `+4.14%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1097470.639 ops/s` | `1064010.104 ops/s` | `-3.05%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `295360.431 ops/s` | `288973.357 ops/s` | `-2.16%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `124881.577 ops/s` | `138560.024 ops/s` | `+10.95%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `170478.854 ops/s` | `150413.333 ops/s` | `-11.77%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `46321.849 ops/s` | `41283.638 ops/s` | `-10.88%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `41029.747 ops/s` | `36066.008 ops/s` | `-12.10%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `5292.102 ops/s` | `5217.630 ops/s` | `-1.41%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `3650.550 ops/s` | `2569.624 ops/s` | `-29.61%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `462.338 ops/s` | `443.645 ops/s` | `-4.04%` | `warning` |

# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `fa915ff050464f4b44f0424ba5745d7dd4343f06`
- Candidate SHA: `e72802bafa68f68a36cf3e5c146ba46f8595a19c`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `1723516.788 ops/s` | `1751667.783 ops/s` | `+1.63%` | `neutral` |
| `segment-index-get-live:getMissSync` | `2485594.497 ops/s` | `2469577.605 ops/s` | `-0.64%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `14070.584 ops/s` | `13017.747 ops/s` | `-7.48%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `2591819.635 ops/s` | `2306443.960 ops/s` | `-11.01%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `108975.286 ops/s` | `110417.893 ops/s` | `+1.32%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `2702002.245 ops/s` | `2563778.010 ops/s` | `-5.12%` | `warning` |
| `segment-index-hot-route-put:putHotRoute` | `1933582.288 ops/s` | `1678522.959 ops/s` | `-13.19%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `981350.244 ops/s` | `982780.626 ops/s` | `+0.15%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `246153.500 ops/s` | `257497.376 ops/s` | `+4.61%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `105559.327 ops/s` | `120112.188 ops/s` | `+13.79%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `140594.173 ops/s` | `137385.188 ops/s` | `-2.28%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `48356.528 ops/s` | `42721.600 ops/s` | `-11.65%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `43032.500 ops/s` | `37523.962 ops/s` | `-12.80%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `5324.028 ops/s` | `5197.638 ops/s` | `-2.37%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `3531.295 ops/s` | `3499.293 ops/s` | `-0.91%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `466.743 ops/s` | `463.021 ops/s` | `-0.80%` | `neutral` |

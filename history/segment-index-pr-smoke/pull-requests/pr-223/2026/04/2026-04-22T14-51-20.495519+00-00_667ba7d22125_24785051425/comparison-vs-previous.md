# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `18a53ed9711d621c676b011285c6999f34435de1`
- Candidate SHA: `667ba7d2212599019dd735e617ec6a79e514f9f1`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2026559.787 ops/s` | `2177713.180 ops/s` | `+7.46%` | `better` |
| `segment-index-get-live:getMissSync` | `2510719.245 ops/s` | `2315925.139 ops/s` | `-7.76%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `13150.255 ops/s` | `14672.277 ops/s` | `+11.57%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `2493042.865 ops/s` | `2444081.725 ops/s` | `-1.96%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `110320.819 ops/s` | `116875.246 ops/s` | `+5.94%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2504982.987 ops/s` | `2408771.903 ops/s` | `-3.84%` | `warning` |
| `segment-index-hot-route-put:putHotRoute` | `1908439.241 ops/s` | `1849857.769 ops/s` | `-3.07%` | `warning` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1086899.196 ops/s` | `1081320.971 ops/s` | `-0.51%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `256901.435 ops/s` | `292430.900 ops/s` | `+13.83%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `91409.029 ops/s` | `123721.459 ops/s` | `+35.35%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `165492.406 ops/s` | `168709.441 ops/s` | `+1.94%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `135076.183 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `128137.530 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `6938.653 ops/s` | `-` | `new` |
| `segment-index-persisted-mutation:deleteSync` | `3674.892 ops/s` | `3623.378 ops/s` | `-1.40%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `3454.188 ops/s` | `3435.651 ops/s` | `-0.54%` | `neutral` |

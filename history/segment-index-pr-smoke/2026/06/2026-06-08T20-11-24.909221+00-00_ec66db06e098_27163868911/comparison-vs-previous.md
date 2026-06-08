# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `c7587cf224789239262f5a67fbe0c9962510a205`
- Candidate SHA: `ec66db06e098be9f665626456c1b5db07b2abb7c`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2289276.340 ops/s` | `2118188.033 ops/s` | `-7.47%` | `worse` |
| `segment-index-get-live:getMissSync` | `2215728.750 ops/s` | `2069470.914 ops/s` | `-6.60%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `1707056.864 ops/s` | `1955676.836 ops/s` | `+14.56%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2237064.330 ops/s` | `2254866.957 ops/s` | `+0.80%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2161273.798 ops/s` | `2113764.173 ops/s` | `-2.20%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1087033.425 ops/s` | `1115454.247 ops/s` | `+2.61%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `312059.172 ops/s` | `299923.260 ops/s` | `-3.89%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `153666.893 ops/s` | `148233.410 ops/s` | `-3.54%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `158392.279 ops/s` | `151689.850 ops/s` | `-4.23%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `171509.019 ops/s` | `166684.853 ops/s` | `-2.81%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `156216.766 ops/s` | `153214.134 ops/s` | `-1.92%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `15292.253 ops/s` | `13470.719 ops/s` | `-11.91%` | `worse` |

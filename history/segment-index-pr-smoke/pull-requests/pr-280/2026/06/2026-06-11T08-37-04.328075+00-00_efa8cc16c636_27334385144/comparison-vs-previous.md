# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `ec66db06e098be9f665626456c1b5db07b2abb7c`
- Candidate SHA: `efa8cc16c636aaa77cbbcc10606a9b3f6ffa4c30`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2351318.324 ops/s` | `2251934.156 ops/s` | `-4.23%` | `warning` |
| `segment-index-get-live:getMissSync` | `2125428.152 ops/s` | `2036732.885 ops/s` | `-4.17%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `1890515.140 ops/s` | `1880965.981 ops/s` | `-0.51%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `2153634.451 ops/s` | `2234733.315 ops/s` | `+3.77%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `1979819.784 ops/s` | `2067710.562 ops/s` | `+4.44%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1149681.351 ops/s` | `1074539.446 ops/s` | `-6.54%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `324700.321 ops/s` | `279088.058 ops/s` | `-14.05%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `156214.115 ops/s` | `127641.331 ops/s` | `-18.29%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `168486.207 ops/s` | `151446.726 ops/s` | `-10.11%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `160095.278 ops/s` | `167262.387 ops/s` | `+4.48%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `145917.286 ops/s` | `152567.207 ops/s` | `+4.56%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `14177.992 ops/s` | `14695.180 ops/s` | `+3.65%` | `better` |

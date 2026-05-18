# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `6d8275b140b9735a85ba77e6dd5fd824362c5cc2`
- Candidate SHA: `9394c17dc9dec4b54d77c860ef60ef562c3d4b8c`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2263401.846 ops/s` | `2170542.941 ops/s` | `-4.10%` | `warning` |
| `segment-index-get-live:getMissSync` | `3401159.846 ops/s` | `3424677.789 ops/s` | `+0.69%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `14164.977 ops/s` | `15138.738 ops/s` | `+6.87%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3577706.120 ops/s` | `3523925.925 ops/s` | `-1.50%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `1812172.143 ops/s` | `2031961.281 ops/s` | `+12.13%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3716051.006 ops/s` | `3718782.362 ops/s` | `+0.07%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `1811154.156 ops/s` | `1942845.519 ops/s` | `+7.27%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1019018.905 ops/s` | `997215.544 ops/s` | `-2.14%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `288063.383 ops/s` | `292882.787 ops/s` | `+1.67%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `108761.966 ops/s` | `105326.148 ops/s` | `-3.16%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `179301.418 ops/s` | `187556.639 ops/s` | `+4.60%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `49007.092 ops/s` | `43800.608 ops/s` | `-10.62%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `43660.757 ops/s` | `38457.672 ops/s` | `-11.92%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `5346.335 ops/s` | `5342.936 ops/s` | `-0.06%` | `neutral` |

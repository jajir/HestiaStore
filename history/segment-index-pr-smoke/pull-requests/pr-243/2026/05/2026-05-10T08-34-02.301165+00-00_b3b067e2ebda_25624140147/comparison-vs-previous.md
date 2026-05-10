# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `438d4c606becfb314b0d1867aa63f4abdba30751`
- Candidate SHA: `b3b067e2ebda395af6b8fd4f8e646f1da3ff6078`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2327072.768 ops/s` | `2487376.688 ops/s` | `+6.89%` | `better` |
| `segment-index-get-live:getMissSync` | `3923572.163 ops/s` | `4046225.924 ops/s` | `+3.13%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `7053.646 ops/s` | `7841.013 ops/s` | `+11.16%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3644912.954 ops/s` | `3883479.042 ops/s` | `+6.55%` | `better` |
| `segment-index-get-persisted:getHitSync` | `122761.108 ops/s` | `117222.546 ops/s` | `-4.51%` | `warning` |
| `segment-index-get-persisted:getMissSync` | `4026586.080 ops/s` | `3862561.740 ops/s` | `-4.07%` | `warning` |
| `segment-index-hot-route-put:putHotRoute` | `1928685.071 ops/s` | `1919201.140 ops/s` | `-0.49%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1042717.687 ops/s` | `1082732.600 ops/s` | `+3.84%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `309122.580 ops/s` | `298359.787 ops/s` | `-3.48%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `139000.099 ops/s` | `143381.458 ops/s` | `+3.15%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `170122.480 ops/s` | `154978.329 ops/s` | `-8.90%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `40761.728 ops/s` | `42923.073 ops/s` | `+5.30%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `35508.101 ops/s` | `37598.178 ops/s` | `+5.89%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `5253.628 ops/s` | `5324.895 ops/s` | `+1.36%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2704.476 ops/s` | `2691.495 ops/s` | `-0.48%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `449.300 ops/s` | `450.213 ops/s` | `+0.20%` | `neutral` |

# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `438d4c606becfb314b0d1867aa63f4abdba30751`
- Candidate SHA: `9dc1ebf892a38ed990f9a8ed6275aa03fb079eb3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2327072.768 ops/s` | `1917838.742 ops/s` | `-17.59%` | `worse` |
| `segment-index-get-live:getMissSync` | `3923572.163 ops/s` | `2560289.025 ops/s` | `-34.75%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `7053.646 ops/s` | `13652.101 ops/s` | `+93.55%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3644912.954 ops/s` | `2436194.412 ops/s` | `-33.16%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `122761.108 ops/s` | `115451.777 ops/s` | `-5.95%` | `warning` |
| `segment-index-get-persisted:getMissSync` | `4026586.080 ops/s` | `2449311.176 ops/s` | `-39.17%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `1928685.071 ops/s` | `1853358.195 ops/s` | `-3.91%` | `warning` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1042717.687 ops/s` | `1021574.264 ops/s` | `-2.03%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `309122.580 ops/s` | `268747.781 ops/s` | `-13.06%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `139000.099 ops/s` | `118644.825 ops/s` | `-14.64%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `170122.480 ops/s` | `150102.956 ops/s` | `-11.77%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `40761.728 ops/s` | `49237.942 ops/s` | `+20.79%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `35508.101 ops/s` | `44015.300 ops/s` | `+23.96%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `5253.628 ops/s` | `5222.641 ops/s` | `-0.59%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2704.476 ops/s` | `2852.470 ops/s` | `+5.47%` | `better` |
| `segment-index-persisted-mutation:putSync` | `449.300 ops/s` | `459.027 ops/s` | `+2.16%` | `neutral` |

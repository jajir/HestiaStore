# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `30f93a9be86df7361bbeb5cf93c3547cbb155d00`
- Candidate SHA: `16312c0c82df89668b236328ba6bd4846edec46b`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2572644.339 ops/s` | `2541799.758 ops/s` | `-1.20%` | `neutral` |
| `segment-index-get-live:getMissSync` | `2327738.679 ops/s` | `2421946.565 ops/s` | `+4.05%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `95.465 ops/s` | `155.436 ops/s` | `+62.82%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `2542548.985 ops/s` | `2413467.689 ops/s` | `-5.08%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `107695.740 ops/s` | `117286.700 ops/s` | `+8.91%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2443250.195 ops/s` | `2523425.124 ops/s` | `+3.28%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2184016.109 ops/s` | `2273198.055 ops/s` | `+4.08%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1135800.737 ops/s` | `1158009.685 ops/s` | `+1.96%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `304176.102 ops/s` | `288778.231 ops/s` | `-5.06%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `114049.835 ops/s` | `125145.311 ops/s` | `+9.73%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `190126.267 ops/s` | `163632.920 ops/s` | `-13.93%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `30249.174 ops/s` | `245402.074 ops/s` | `+711.27%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `22770.642 ops/s` | `25696.810 ops/s` | `+12.85%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `7478.532 ops/s` | `219705.264 ops/s` | `+2837.81%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `3102.285 ops/s` | `3190.590 ops/s` | `+2.85%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `2997.065 ops/s` | `2936.892 ops/s` | `-2.01%` | `neutral` |

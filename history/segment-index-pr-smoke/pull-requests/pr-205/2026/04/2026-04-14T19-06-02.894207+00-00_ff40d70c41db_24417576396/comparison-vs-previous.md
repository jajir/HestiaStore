# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `435bb29609f064befe1e1c40df2c191ede262526`
- Candidate SHA: `ff40d70c41db5d6bfb2af076123dcfe2714e0f38`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `3619205.705 ops/s` | `3467641.651 ops/s` | `-4.19%` | `warning` |
| `segment-index-get-live:getMissSync` | `4242918.553 ops/s` | `4001406.805 ops/s` | `-5.69%` | `warning` |
| `segment-index-get-multisegment-hot:getHitSync` | `89.780 ops/s` | `81.150 ops/s` | `-9.61%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `3547470.222 ops/s` | `4010772.441 ops/s` | `+13.06%` | `better` |
| `segment-index-get-persisted:getHitSync` | `115853.905 ops/s` | `106234.061 ops/s` | `-8.30%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `3841448.840 ops/s` | `3933427.223 ops/s` | `+2.39%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2609036.932 ops/s` | `2702300.951 ops/s` | `+3.57%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1472098.144 ops/s` | `1328567.716 ops/s` | `-9.75%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `336149.467 ops/s` | `334960.447 ops/s` | `-0.35%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `183479.804 ops/s` | `177743.035 ops/s` | `-3.13%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `152669.663 ops/s` | `157217.413 ops/s` | `+2.98%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `25205.653 ops/s` | `24242.822 ops/s` | `-3.82%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `16418.753 ops/s` | `19126.823 ops/s` | `+16.49%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `8786.900 ops/s` | `5116.000 ops/s` | `-41.78%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `2461.561 ops/s` | `2703.635 ops/s` | `+9.83%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2336.597 ops/s` | `2604.640 ops/s` | `+11.47%` | `better` |

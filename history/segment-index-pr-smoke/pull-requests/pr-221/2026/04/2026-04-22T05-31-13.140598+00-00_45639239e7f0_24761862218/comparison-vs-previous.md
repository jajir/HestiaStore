# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `b8e458aa920967b0a677589b9cdfeac52bc6d7bf`
- Candidate SHA: `45639239e7f043ac91a6c5a7f8064c45f8902096`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `1924356.806 ops/s` | `1936293.632 ops/s` | `+0.62%` | `neutral` |
| `segment-index-get-live:getMissSync` | `2497693.025 ops/s` | `2467338.754 ops/s` | `-1.22%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `14317.674 ops/s` | `14509.352 ops/s` | `+1.34%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `2510947.016 ops/s` | `2578393.839 ops/s` | `+2.69%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `116428.960 ops/s` | `115940.933 ops/s` | `-0.42%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `2610836.131 ops/s` | `2433885.402 ops/s` | `-6.78%` | `warning` |
| `segment-index-hot-route-put:putHotRoute` | `2050542.102 ops/s` | `2006314.149 ops/s` | `-2.16%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1095851.872 ops/s` | `1077241.451 ops/s` | `-1.70%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `307169.670 ops/s` | `279118.449 ops/s` | `-9.13%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `135183.036 ops/s` | `107499.815 ops/s` | `-20.48%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `171986.634 ops/s` | `171618.634 ops/s` | `-0.21%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `148513.607 ops/s` | `141089.848 ops/s` | `-5.00%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `142106.907 ops/s` | `134442.745 ops/s` | `-5.39%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `6406.701 ops/s` | `6647.102 ops/s` | `+3.75%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `3831.918 ops/s` | `3824.847 ops/s` | `-0.18%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `3744.730 ops/s` | `3718.640 ops/s` | `-0.70%` | `neutral` |

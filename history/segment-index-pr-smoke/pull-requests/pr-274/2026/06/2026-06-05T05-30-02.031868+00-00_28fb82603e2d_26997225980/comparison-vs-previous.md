# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `1d013e75d1cb1d4624c28c09ef20598b553fcfb9`
- Candidate SHA: `28fb82603e2db372c324277c37dfacd122552c5b`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2206507.345 ops/s` | `2158669.738 ops/s` | `-2.17%` | `neutral` |
| `segment-index-get-live:getMissSync` | `2024305.444 ops/s` | `1997525.924 ops/s` | `-1.32%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `1720990.858 ops/s` | `1850525.931 ops/s` | `+7.53%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2071964.411 ops/s` | `1993520.981 ops/s` | `-3.79%` | `warning` |
| `segment-index-hot-route-put:putHotRoute` | `2130156.423 ops/s` | `2030306.507 ops/s` | `-4.69%` | `warning` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1062076.551 ops/s` | `1134220.465 ops/s` | `+6.79%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `289165.201 ops/s` | `277365.751 ops/s` | `-4.08%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `135267.575 ops/s` | `136461.299 ops/s` | `+0.88%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `153897.627 ops/s` | `140904.452 ops/s` | `-8.44%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `181772.522 ops/s` | `180250.138 ops/s` | `-0.84%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `166574.002 ops/s` | `165823.451 ops/s` | `-0.45%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `15198.520 ops/s` | `14426.687 ops/s` | `-5.08%` | `warning` |

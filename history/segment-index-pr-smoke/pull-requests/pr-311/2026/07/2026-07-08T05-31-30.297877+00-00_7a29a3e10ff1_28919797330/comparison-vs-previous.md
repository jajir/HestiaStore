# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `1a6bf579500a142c3427c80510c8c48230fa9e25`
- Candidate SHA: `7a29a3e10ff180ccf39bb6dbb7b0097328290082`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2205647.396 ops/s` | `2172667.734 ops/s` | `-1.50%` | `neutral` |
| `segment-index-get-live:getMissSync` | `2146829.590 ops/s` | `2047543.610 ops/s` | `-4.62%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `2023420.460 ops/s` | `1961163.914 ops/s` | `-3.08%` | `warning` |
| `segment-index-get-persisted:getMissSync` | `2092670.989 ops/s` | `2039506.713 ops/s` | `-2.54%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2145442.750 ops/s` | `2207445.761 ops/s` | `+2.89%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1094990.733 ops/s` | `1082050.828 ops/s` | `-1.18%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `472878.420 ops/s` | `455619.626 ops/s` | `-3.65%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `321869.790 ops/s` | `307656.004 ops/s` | `-4.42%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `151008.631 ops/s` | `147963.622 ops/s` | `-2.02%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `635669.507 ops/s` | `377110.027 ops/s` | `-40.68%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `621197.952 ops/s` | `364747.190 ops/s` | `-41.28%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `14471.555 ops/s` | `12362.837 ops/s` | `-14.57%` | `worse` |

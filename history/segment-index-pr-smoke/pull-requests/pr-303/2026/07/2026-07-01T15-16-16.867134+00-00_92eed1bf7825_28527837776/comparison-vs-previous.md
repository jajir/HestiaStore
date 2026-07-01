# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `67bbf8e34d2b602923270722df932c97760ccab3`
- Candidate SHA: `92eed1bf7825eaa533ce6d7312b5b62c4eb600b6`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2268171.114 ops/s` | `2303527.912 ops/s` | `+1.56%` | `neutral` |
| `segment-index-get-live:getMissSync` | `2266313.109 ops/s` | `1975178.928 ops/s` | `-12.85%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `2003315.023 ops/s` | `1691383.049 ops/s` | `-15.57%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `2018888.843 ops/s` | `2136545.441 ops/s` | `+5.83%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2185393.632 ops/s` | `2081144.870 ops/s` | `-4.77%` | `warning` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1074840.718 ops/s` | `1126149.079 ops/s` | `+4.77%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `429939.968 ops/s` | `483766.538 ops/s` | `+12.52%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `269884.190 ops/s` | `321224.257 ops/s` | `+19.02%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `160055.777 ops/s` | `162542.282 ops/s` | `+1.55%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `599587.258 ops/s` | `549352.577 ops/s` | `-8.38%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `585457.106 ops/s` | `535905.094 ops/s` | `-8.46%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `14130.153 ops/s` | `13447.483 ops/s` | `-4.83%` | `warning` |

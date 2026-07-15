# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `8b88b599997497df9b956d4be577a2b7424cedcb`
- Candidate SHA: `bd87c6daa53b23c327417a79515ffef638fa62a9`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2310962.562 ops/s` | `2200640.763 ops/s` | `-4.77%` | `warning` |
| `segment-index-get-live:getMissSync` | `2108684.289 ops/s` | `2190346.532 ops/s` | `+3.87%` | `better` |
| `segment-index-get-persisted:getHitSync` | `1764544.543 ops/s` | `1957563.699 ops/s` | `+10.94%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2241755.204 ops/s` | `2190252.891 ops/s` | `-2.30%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2127264.249 ops/s` | `2165218.699 ops/s` | `+1.78%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1065311.465 ops/s` | `1086906.043 ops/s` | `+2.03%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `462606.254 ops/s` | `446543.984 ops/s` | `-3.47%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `302205.735 ops/s` | `276253.106 ops/s` | `-8.59%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `160400.519 ops/s` | `170290.878 ops/s` | `+6.17%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `596125.359 ops/s` | `595790.867 ops/s` | `-0.06%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `581364.328 ops/s` | `580421.083 ops/s` | `-0.16%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `14761.031 ops/s` | `15369.784 ops/s` | `+4.12%` | `better` |

# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `1756ecbfaee92319c50abf1110e55f35b5e37fd0`
- Candidate SHA: `e0ba10acae826f7b78150611f6690e07e5fb9359`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2195768.886 ops/s` | `2210176.658 ops/s` | `+0.66%` | `neutral` |
| `segment-index-get-live:getMissSync` | `1904801.534 ops/s` | `2033206.809 ops/s` | `+6.74%` | `better` |
| `segment-index-get-persisted:getHitSync` | `1714865.195 ops/s` | `1979661.902 ops/s` | `+15.44%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2264765.533 ops/s` | `2078483.494 ops/s` | `-8.23%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `2175585.276 ops/s` | `1980572.698 ops/s` | `-8.96%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1121604.968 ops/s` | `1094683.156 ops/s` | `-2.40%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `288729.749 ops/s` | `315467.615 ops/s` | `+9.26%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `123399.728 ops/s` | `142036.882 ops/s` | `+15.10%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `165330.021 ops/s` | `173430.733 ops/s` | `+4.90%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `179692.071 ops/s` | `166078.193 ops/s` | `-7.58%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `165297.624 ops/s` | `151163.004 ops/s` | `-8.55%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `14394.447 ops/s` | `14915.189 ops/s` | `+3.62%` | `better` |

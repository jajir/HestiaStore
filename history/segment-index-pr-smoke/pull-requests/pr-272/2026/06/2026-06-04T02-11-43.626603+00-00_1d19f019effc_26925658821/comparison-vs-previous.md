# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `d5ffeeaaccc81e4004484e8650d5b9d7fd25e529`
- Candidate SHA: `1d19f019effc504b2743711f9b3e6cc60f337b49`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2172343.934 ops/s` | `2230504.578 ops/s` | `+2.68%` | `neutral` |
| `segment-index-get-live:getMissSync` | `2018666.262 ops/s` | `2149895.609 ops/s` | `+6.50%` | `better` |
| `segment-index-get-persisted:getHitSync` | `1842607.168 ops/s` | `1622167.394 ops/s` | `-11.96%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `2159614.825 ops/s` | `2158194.689 ops/s` | `-0.07%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2084826.260 ops/s` | `2069491.620 ops/s` | `-0.74%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1138924.260 ops/s` | `1151546.995 ops/s` | `+1.11%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `296774.497 ops/s` | `319545.508 ops/s` | `+7.67%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `168924.652 ops/s` | `174746.922 ops/s` | `+3.45%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `127849.844 ops/s` | `144798.586 ops/s` | `+13.26%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `156426.629 ops/s` | `152776.181 ops/s` | `-2.33%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `142868.504 ops/s` | `138680.166 ops/s` | `-2.93%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `13558.125 ops/s` | `14096.015 ops/s` | `+3.97%` | `better` |

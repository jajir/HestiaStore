# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `1a6bf579500a142c3427c80510c8c48230fa9e25`
- Candidate SHA: `c4e36341d5090cbedaf4272dfe8a9d7ca1569f19`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2382101.337 ops/s` | `2182318.313 ops/s` | `-8.39%` | `worse` |
| `segment-index-get-live:getMissSync` | `2276507.655 ops/s` | `2175897.083 ops/s` | `-4.42%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `1747501.573 ops/s` | `2026515.442 ops/s` | `+15.97%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2139698.385 ops/s` | `2203195.003 ops/s` | `+2.97%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2146024.412 ops/s` | `2119048.495 ops/s` | `-1.26%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1133818.805 ops/s` | `1113849.243 ops/s` | `-1.76%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `434905.934 ops/s` | `459817.803 ops/s` | `+5.73%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `272525.578 ops/s` | `300685.238 ops/s` | `+10.33%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `162380.356 ops/s` | `159132.565 ops/s` | `-2.00%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `590441.652 ops/s` | `590214.704 ops/s` | `-0.04%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `575418.604 ops/s` | `574956.188 ops/s` | `-0.08%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `15023.048 ops/s` | `15258.516 ops/s` | `+1.57%` | `neutral` |

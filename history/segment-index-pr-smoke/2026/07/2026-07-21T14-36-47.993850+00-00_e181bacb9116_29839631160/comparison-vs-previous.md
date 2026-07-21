# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `827657fe930eb1d550431967aadf932ddf12455c`
- Candidate SHA: `e181bacb91161177cbeddbbc4d92d9884c1095d5`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2568512.310 ops/s` | `2446713.356 ops/s` | `-4.74%` | `warning` |
| `segment-index-get-live:getMissSync` | `1994733.176 ops/s` | `2063027.194 ops/s` | `+3.42%` | `better` |
| `segment-index-get-persisted:getHitSync` | `2099344.404 ops/s` | `1842024.454 ops/s` | `-12.26%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `2255788.962 ops/s` | `2308642.356 ops/s` | `+2.34%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2228246.705 ops/s` | `2194368.973 ops/s` | `-1.52%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1172126.347 ops/s` | `1133064.866 ops/s` | `-3.33%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `467923.084 ops/s` | `456722.744 ops/s` | `-2.39%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `309516.294 ops/s` | `295657.592 ops/s` | `-4.48%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `158406.790 ops/s` | `161065.152 ops/s` | `+1.68%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `671841.367 ops/s` | `703737.544 ops/s` | `+4.75%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `657062.855 ops/s` | `688455.560 ops/s` | `+4.78%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `14778.512 ops/s` | `15281.984 ops/s` | `+3.41%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `-` | `2480.156 ops/s` | `-` | `new` |
| `segment-index-persisted-mutation:putSync` | `-` | `2388.452 ops/s` | `-` | `new` |

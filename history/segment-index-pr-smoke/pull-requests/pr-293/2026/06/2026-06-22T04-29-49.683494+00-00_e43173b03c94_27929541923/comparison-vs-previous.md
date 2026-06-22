# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `cb23b15e3542f2dcf618e20cb692494f55e16165`
- Candidate SHA: `e43173b03c94ad038d294cfece5724fc2877d862`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2228389.284 ops/s` | `2239527.760 ops/s` | `+0.50%` | `neutral` |
| `segment-index-get-live:getMissSync` | `1915953.106 ops/s` | `1891869.242 ops/s` | `-1.26%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `1720181.119 ops/s` | `1800114.812 ops/s` | `+4.65%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2015984.769 ops/s` | `1980600.217 ops/s` | `-1.76%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2063365.089 ops/s` | `2038778.016 ops/s` | `-1.19%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1088781.747 ops/s` | `1066784.064 ops/s` | `-2.02%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `295343.464 ops/s` | `302749.579 ops/s` | `+2.51%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `116938.488 ops/s` | `153960.602 ops/s` | `+31.66%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `178404.976 ops/s` | `148788.978 ops/s` | `-16.60%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `166646.519 ops/s` | `176534.881 ops/s` | `+5.93%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `152467.757 ops/s` | `162080.673 ops/s` | `+6.30%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `14178.761 ops/s` | `14454.208 ops/s` | `+1.94%` | `neutral` |

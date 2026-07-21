# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `e181bacb91161177cbeddbbc4d92d9884c1095d5`
- Candidate SHA: `f77d9f36b6a44cdd36ce858c25c4df8e677009b9`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2310897.163 ops/s` | `4668409.840 ops/s` | `+102.02%` | `better` |
| `segment-index-get-live:getMissSync` | `2244687.121 ops/s` | `4103973.595 ops/s` | `+82.83%` | `better` |
| `segment-index-get-persisted:getHitSync` | `2025554.997 ops/s` | `3135610.872 ops/s` | `+54.80%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2135209.393 ops/s` | `4448192.834 ops/s` | `+108.33%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2315854.378 ops/s` | `3646592.776 ops/s` | `+57.46%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1145436.328 ops/s` | `1816037.578 ops/s` | `+58.55%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `459687.578 ops/s` | `551741.185 ops/s` | `+20.03%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `288685.117 ops/s` | `374460.192 ops/s` | `+29.71%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `171002.461 ops/s` | `177280.993 ops/s` | `+3.67%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `654202.305 ops/s` | `866194.080 ops/s` | `+32.40%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `639569.274 ops/s` | `852265.164 ops/s` | `+33.26%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `14633.031 ops/s` | `13928.916 ops/s` | `-4.81%` | `warning` |
| `segment-index-persisted-mutation:deleteSync` | `3493.454 ops/s` | `3502.149 ops/s` | `+0.25%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `3414.010 ops/s` | `3370.772 ops/s` | `-1.27%` | `neutral` |

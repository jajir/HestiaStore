# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `21d1da156bc01e2f39b9f119c5882d79b3da438a`
- Candidate SHA: `dce1fbc911dbad09d8df685697a7c8b43069ed09`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2315911.539 ops/s` | `2016872.339 ops/s` | `-12.91%` | `worse` |
| `segment-index-get-live:getMissSync` | `1863979.147 ops/s` | `1926117.955 ops/s` | `+3.33%` | `better` |
| `segment-index-get-persisted:getHitSync` | `1451345.055 ops/s` | `1506270.796 ops/s` | `+3.78%` | `better` |
| `segment-index-get-persisted:getMissSync` | `1991482.201 ops/s` | `1876016.337 ops/s` | `-5.80%` | `warning` |
| `segment-index-hot-route-put:putHotRoute` | `1925504.502 ops/s` | `2148073.099 ops/s` | `+11.56%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1092502.223 ops/s` | `1153439.869 ops/s` | `+5.58%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `260.226 ms/op` | `260.393 ms/op` | `+0.06%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `283.974 ms/op` | `282.452 ms/op` | `-0.54%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `256.019 ms/op` | `258.678 ms/op` | `+1.04%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `430526.981 ops/s` | `436893.516 ops/s` | `+1.48%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `196044.947 ops/s` | `198424.447 ops/s` | `+1.21%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `234482.033 ops/s` | `238469.069 ops/s` | `+1.70%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `888865.779 ops/s` | `871546.707 ops/s` | `-1.95%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `872431.893 ops/s` | `854779.051 ops/s` | `-2.02%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `16433.886 ops/s` | `16767.656 ops/s` | `+2.03%` | `neutral` |

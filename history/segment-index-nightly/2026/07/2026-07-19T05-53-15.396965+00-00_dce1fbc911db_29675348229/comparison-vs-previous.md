# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `21d1da156bc01e2f39b9f119c5882d79b3da438a`
- Candidate SHA: `dce1fbc911dbad09d8df685697a7c8b43069ed09`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2023975.166 ops/s` | `2161392.559 ops/s` | `+6.79%` | `better` |
| `segment-index-get-live:getMissSync` | `2099197.080 ops/s` | `2039626.323 ops/s` | `-2.84%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `1657386.464 ops/s` | `2369155.569 ops/s` | `+42.95%` | `better` |
| `segment-index-get-persisted:getMissSync` | `1867225.980 ops/s` | `1911018.156 ops/s` | `+2.35%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2131153.097 ops/s` | `2042441.115 ops/s` | `-4.16%` | `warning` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1131935.046 ops/s` | `1082295.769 ops/s` | `-4.39%` | `warning` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `259.608 ms/op` | `259.071 ms/op` | `-0.21%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `280.504 ms/op` | `283.924 ms/op` | `+1.22%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `253.984 ms/op` | `254.454 ms/op` | `+0.19%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `415299.090 ops/s` | `424919.086 ops/s` | `+2.32%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `185086.006 ops/s` | `199641.554 ops/s` | `+7.86%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `230213.084 ops/s` | `225277.532 ops/s` | `-2.14%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `821490.789 ops/s` | `884675.779 ops/s` | `+7.69%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `804506.783 ops/s` | `867087.658 ops/s` | `+7.78%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `16984.006 ops/s` | `17588.121 ops/s` | `+3.56%` | `better` |

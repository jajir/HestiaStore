# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `21d1da156bc01e2f39b9f119c5882d79b3da438a`
- Candidate SHA: `dce1fbc911dbad09d8df685697a7c8b43069ed09`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `1606913.964 ops/s` | `1566962.495 ops/s` | `-2.49%` | `neutral` |
| `segment-index-get-live:getMissSync` | `1465078.404 ops/s` | `1368619.028 ops/s` | `-6.58%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `1364114.994 ops/s` | `1327949.459 ops/s` | `-2.65%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `1439209.563 ops/s` | `1493276.566 ops/s` | `+3.76%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `1994563.229 ops/s` | `2022503.697 ops/s` | `+1.40%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1006229.671 ops/s` | `1012706.397 ops/s` | `+0.64%` | `neutral` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `139.213 ms/op` | `139.102 ms/op` | `-0.08%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `161.364 ms/op` | `162.070 ms/op` | `+0.44%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `136.495 ms/op` | `136.701 ms/op` | `+0.15%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `399508.338 ops/s` | `403409.634 ops/s` | `+0.98%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `164745.572 ops/s` | `166949.617 ops/s` | `+1.34%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `234762.765 ops/s` | `236460.017 ops/s` | `+0.72%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `855438.862 ops/s` | `844768.582 ops/s` | `-1.25%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `836150.673 ops/s` | `825407.896 ops/s` | `-1.28%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `19288.189 ops/s` | `19360.686 ops/s` | `+0.38%` | `neutral` |

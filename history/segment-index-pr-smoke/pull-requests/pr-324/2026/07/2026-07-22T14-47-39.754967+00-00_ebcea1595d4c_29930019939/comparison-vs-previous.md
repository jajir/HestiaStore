# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `dce1fbc911dbad09d8df685697a7c8b43069ed09`
- Candidate SHA: `ebcea1595d4c6182706205faa8c3bb70ae467f58`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2039168.332 ops/s` | `2044824.134 ops/s` | `+0.28%` | `neutral` |
| `segment-index-get-live:getMissSync` | `1907926.731 ops/s` | `1886831.815 ops/s` | `-1.11%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `1691784.246 ops/s` | `1876962.397 ops/s` | `+10.95%` | `better` |
| `segment-index-get-persisted:getMissSync` | `1907038.023 ops/s` | `1794350.752 ops/s` | `-5.91%` | `warning` |
| `segment-index-hot-route-put:putHotRoute` | `2761521.002 ops/s` | `2794486.528 ops/s` | `+1.19%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1372158.609 ops/s` | `1365907.714 ops/s` | `-0.46%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `480403.981 ops/s` | `482647.780 ops/s` | `+0.47%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `256575.173 ops/s` | `257294.253 ops/s` | `+0.28%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `223828.809 ops/s` | `225353.527 ops/s` | `+0.68%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `803047.377 ops/s` | `794881.824 ops/s` | `-1.02%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `780150.434 ops/s` | `772966.529 ops/s` | `-0.92%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `22896.943 ops/s` | `21915.295 ops/s` | `-4.29%` | `warning` |

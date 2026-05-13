# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `e942e650381bf00db7c2fbb3790ab0c49b708f39`
- Candidate SHA: `2065f770d3cc12731652f51f3a824a9fddfd39a6`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2321667.872 ops/s` | `2003607.728 ops/s` | `-13.70%` | `worse` |
| `segment-index-get-live:getMissSync` | `3476585.043 ops/s` | `3460734.886 ops/s` | `-0.46%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `7398.638 ops/s` | `7527.152 ops/s` | `+1.74%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3450100.298 ops/s` | `3568176.886 ops/s` | `+3.42%` | `better` |
| `segment-index-get-persisted:getHitSync` | `2153337.453 ops/s` | `1581370.868 ops/s` | `-26.56%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `3543561.642 ops/s` | `3403335.629 ops/s` | `-3.96%` | `warning` |
| `segment-index-hot-route-put:putHotRoute` | `1985866.328 ops/s` | `1961843.559 ops/s` | `-1.21%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1062130.012 ops/s` | `1072755.685 ops/s` | `+1.00%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `319140.499 ops/s` | `277282.569 ops/s` | `-13.12%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `143055.871 ops/s` | `110925.147 ops/s` | `-22.46%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `176084.629 ops/s` | `166357.422 ops/s` | `-5.52%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `47997.575 ops/s` | `47411.676 ops/s` | `-1.22%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `42685.350 ops/s` | `42073.886 ops/s` | `-1.43%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `5312.225 ops/s` | `5337.790 ops/s` | `+0.48%` | `neutral` |

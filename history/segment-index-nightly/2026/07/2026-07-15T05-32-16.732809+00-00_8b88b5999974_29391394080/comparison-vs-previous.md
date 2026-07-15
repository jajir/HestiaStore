# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `1e37c5925410c7e6649b1fa1d4fd33325bcb2d4c`
- Candidate SHA: `8b88b599997497df9b956d4be577a2b7424cedcb`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2184523.053 ops/s` | `2096099.651 ops/s` | `-4.05%` | `warning` |
| `segment-index-get-live:getMissSync` | `1864433.822 ops/s` | `2064986.577 ops/s` | `+10.76%` | `better` |
| `segment-index-get-persisted:getHitSync` | `1611407.447 ops/s` | `1585991.076 ops/s` | `-1.58%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `1928753.650 ops/s` | `1837202.296 ops/s` | `-4.75%` | `warning` |
| `segment-index-hot-route-put:putHotRoute` | `2116658.711 ops/s` | `2071197.476 ops/s` | `-2.15%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1013677.488 ops/s` | `1107378.695 ops/s` | `+9.24%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `258.047 ms/op` | `256.403 ms/op` | `-0.64%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `276.818 ms/op` | `276.087 ms/op` | `-0.26%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `252.802 ms/op` | `253.857 ms/op` | `+0.42%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `432429.350 ops/s` | `445050.815 ops/s` | `+2.92%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `190289.209 ops/s` | `206929.432 ops/s` | `+8.74%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `242140.141 ops/s` | `238121.383 ops/s` | `-1.66%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `899058.177 ops/s` | `859012.943 ops/s` | `-4.45%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `882184.350 ops/s` | `841985.175 ops/s` | `-4.56%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `16873.827 ops/s` | `17027.767 ops/s` | `+0.91%` | `neutral` |

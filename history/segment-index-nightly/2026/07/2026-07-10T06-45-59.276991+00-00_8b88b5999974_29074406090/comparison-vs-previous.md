# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `1e37c5925410c7e6649b1fa1d4fd33325bcb2d4c`
- Candidate SHA: `8b88b599997497df9b956d4be577a2b7424cedcb`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2919958.456 ops/s` | `2054882.404 ops/s` | `-29.63%` | `worse` |
| `segment-index-get-live:getMissSync` | `1832842.320 ops/s` | `1900768.222 ops/s` | `+3.71%` | `better` |
| `segment-index-get-persisted:getHitSync` | `2433458.484 ops/s` | `1484103.604 ops/s` | `-39.01%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `2876245.610 ops/s` | `2946628.816 ops/s` | `+2.45%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2123708.919 ops/s` | `2113487.895 ops/s` | `-0.48%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1043577.085 ops/s` | `1100683.384 ops/s` | `+5.47%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `306.584 ms/op` | `305.817 ms/op` | `-0.25%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `332.953 ms/op` | `330.825 ms/op` | `-0.64%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `304.002 ms/op` | `298.783 ms/op` | `-1.72%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `432867.358 ops/s` | `443927.193 ops/s` | `+2.56%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `188876.891 ops/s` | `197802.351 ops/s` | `+4.73%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `243990.467 ops/s` | `246124.842 ops/s` | `+0.87%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `865631.924 ops/s` | `870065.230 ops/s` | `+0.51%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `848352.933 ops/s` | `852853.825 ops/s` | `+0.53%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `17278.990 ops/s` | `17211.406 ops/s` | `-0.39%` | `neutral` |

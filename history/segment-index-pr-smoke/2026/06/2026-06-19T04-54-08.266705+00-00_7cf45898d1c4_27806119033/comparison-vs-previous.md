# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `18051a49f409c9a5b2da3e57aa8ae99815392788`
- Candidate SHA: `7cf45898d1c4e656791b1a025e36fee94e8195ca`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2236197.187 ops/s` | `2169438.622 ops/s` | `-2.99%` | `neutral` |
| `segment-index-get-live:getMissSync` | `2208904.760 ops/s` | `2200942.072 ops/s` | `-0.36%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `1988809.802 ops/s` | `2093895.865 ops/s` | `+5.28%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2083882.336 ops/s` | `2147793.408 ops/s` | `+3.07%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2140647.765 ops/s` | `2107428.614 ops/s` | `-1.55%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1100654.279 ops/s` | `1085704.916 ops/s` | `-1.36%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `303268.004 ops/s` | `322876.177 ops/s` | `+6.47%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `141133.918 ops/s` | `146467.162 ops/s` | `+3.78%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `162134.086 ops/s` | `176409.016 ops/s` | `+8.80%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `184386.012 ops/s` | `178634.458 ops/s` | `-3.12%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `169447.932 ops/s` | `163872.507 ops/s` | `-3.29%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `14938.080 ops/s` | `14761.950 ops/s` | `-1.18%` | `neutral` |

# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `a63e8857313f97e6163f9e4567b7002fa0a469ea`
- Candidate SHA: `96593cd49b374380016362f1fc4d5bacdf709832`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2514892.995 ops/s` | `2217443.760 ops/s` | `-11.83%` | `worse` |
| `segment-index-get-live:getMissSync` | `1763534.492 ops/s` | `1872326.991 ops/s` | `+6.17%` | `better` |
| `segment-index-get-persisted:getHitSync` | `1706421.803 ops/s` | `1414240.468 ops/s` | `-17.12%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `1879499.398 ops/s` | `1898078.218 ops/s` | `+0.99%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2068260.674 ops/s` | `2144220.895 ops/s` | `+3.67%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `938101.080 ops/s` | `1080490.499 ops/s` | `+15.18%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `260.536 ms/op` | `259.884 ms/op` | `-0.25%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `281.499 ms/op` | `280.107 ms/op` | `-0.49%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `256.113 ms/op` | `257.512 ms/op` | `+0.55%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `425972.484 ops/s` | `427996.230 ops/s` | `+0.48%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `191017.596 ops/s` | `192554.577 ops/s` | `+0.80%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `234954.888 ops/s` | `235441.652 ops/s` | `+0.21%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `837827.637 ops/s` | `906329.252 ops/s` | `+8.18%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `819865.486 ops/s` | `889817.171 ops/s` | `+8.53%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `17962.151 ops/s` | `16512.081 ops/s` | `-8.07%` | `worse` |

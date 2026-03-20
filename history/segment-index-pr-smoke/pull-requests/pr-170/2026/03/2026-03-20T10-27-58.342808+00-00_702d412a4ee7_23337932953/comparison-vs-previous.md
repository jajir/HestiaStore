# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `ec14f87a3a2d8100d259761a91d659b7d5a9a8cc`
- Candidate SHA: `702d412a4ee730b578fad6eadf534d988e1f2557`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `84.452 ops/s` | `94.348 ops/s` | `+11.72%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `86.414 ops/s` | `82.308 ops/s` | `-4.75%` | `warning` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `173514.643 ops/s` | `175846.673 ops/s` | `+1.34%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `4087347.613 ops/s` | `3747906.566 ops/s` | `-8.30%` | `worse` |
| `segment-index-get-overlay:getHitAsyncJoin` | `169858.698 ops/s` | `171115.352 ops/s` | `+0.74%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `4210664.851 ops/s` | `4142575.756 ops/s` | `-1.62%` | `neutral` |
| `segment-index-get-overlay:getMissAsyncJoin` | `174255.230 ops/s` | `175306.028 ops/s` | `+0.60%` | `neutral` |
| `segment-index-get-overlay:getMissSync` | `3756775.676 ops/s` | `3910513.360 ops/s` | `+4.09%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `52392.156 ops/s` | `56388.011 ops/s` | `+7.63%` | `better` |
| `segment-index-get-persisted:getHitSync` | `103904.666 ops/s` | `107314.557 ops/s` | `+3.28%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `174199.799 ops/s` | `164289.294 ops/s` | `-5.69%` | `warning` |
| `segment-index-get-persisted:getMissSync` | `3983689.890 ops/s` | `3995520.254 ops/s` | `+0.30%` | `neutral` |
| `segment-index-hot-partition-put:putHotPartition` | `2931843.496 ops/s` | `3042000.215 ops/s` | `+3.76%` | `better` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1664733.566 ops/s` | `1642654.653 ops/s` | `-1.33%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `432922.668 ops/s` | `407566.877 ops/s` | `-5.86%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `427845.295 ops/s` | `402379.803 ops/s` | `-5.95%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5077.373 ops/s` | `5187.074 ops/s` | `+2.16%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `196449.016 ops/s` | `189109.081 ops/s` | `-3.74%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `194145.689 ops/s` | `186539.508 ops/s` | `-3.92%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `2303.328 ops/s` | `2569.573 ops/s` | `+11.56%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `1529.480 ops/s` | `1744.517 ops/s` | `+14.06%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `1684.320 ops/s` | `1843.478 ops/s` | `+9.45%` | `better` |
| `segment-index-persisted-mutation:putAsyncJoin` | `1513.819 ops/s` | `1726.928 ops/s` | `+14.08%` | `better` |
| `segment-index-persisted-mutation:putSync` | `1696.112 ops/s` | `1860.550 ops/s` | `+9.69%` | `better` |

# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `9d5a339a22a9b41105834c8ae9ce3405bf76481c`
- Candidate SHA: `7201e16d88a027d8a2c3b37f10108c723f9fe11d`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `87.887 ops/s` | `89.747 ops/s` | `+2.12%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `90.182 ops/s` | `95.486 ops/s` | `+5.88%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `165003.911 ops/s` | `173545.384 ops/s` | `+5.18%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3842175.204 ops/s` | `3868102.415 ops/s` | `+0.67%` | `neutral` |
| `segment-index-get-overlay:getHitAsyncJoin` | `156383.765 ops/s` | `164046.622 ops/s` | `+4.90%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4242771.883 ops/s` | `4196455.663 ops/s` | `-1.09%` | `neutral` |
| `segment-index-get-overlay:getMissAsyncJoin` | `158384.833 ops/s` | `164423.877 ops/s` | `+3.81%` | `better` |
| `segment-index-get-overlay:getMissSync` | `3927878.358 ops/s` | `4044055.864 ops/s` | `+2.96%` | `neutral` |
| `segment-index-get-persisted:getHitAsyncJoin` | `56455.848 ops/s` | `52409.242 ops/s` | `-7.17%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `106054.215 ops/s` | `111406.277 ops/s` | `+5.05%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `163018.136 ops/s` | `161666.257 ops/s` | `-0.83%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `3859341.319 ops/s` | `4175833.934 ops/s` | `+8.20%` | `better` |
| `segment-index-hot-partition-put:putHotPartition` | `3028321.041 ops/s` | `2952047.670 ops/s` | `-2.52%` | `neutral` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1664173.925 ops/s` | `1547807.848 ops/s` | `-6.99%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `407462.667 ops/s` | `355580.436 ops/s` | `-12.73%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `402274.749 ops/s` | `350651.993 ops/s` | `-12.83%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5187.918 ops/s` | `4928.444 ops/s` | `-5.00%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `200918.194 ops/s` | `171552.716 ops/s` | `-14.62%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `198679.987 ops/s` | `169055.494 ops/s` | `-14.91%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `2238.208 ops/s` | `2497.221 ops/s` | `+11.57%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2270.552 ops/s` | `2069.297 ops/s` | `-8.86%` | `worse` |
| `segment-index-persisted-mutation:deleteAsyncJoin:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteAsyncJoin:diag_fileBytesDelta` | `54953.757 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteAsyncJoin:diag_fileCountDelta` | `3.666 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteSync` | `2520.489 ops/s` | `2432.170 ops/s` | `-3.50%` | `warning` |
| `segment-index-persisted-mutation:deleteSync:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteSync:diag_fileBytesDelta` | `56896.113 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteSync:diag_fileCountDelta` | `2.966 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2280.671 ops/s` | `1959.175 ops/s` | `-14.10%` | `worse` |
| `segment-index-persisted-mutation:putAsyncJoin:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putAsyncJoin:diag_fileBytesDelta` | `234202.224 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putAsyncJoin:diag_fileCountDelta` | `3.663 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putSync` | `2517.162 ops/s` | `2037.301 ops/s` | `-19.06%` | `worse` |
| `segment-index-persisted-mutation:putSync:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putSync:diag_fileBytesDelta` | `261527.476 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putSync:diag_fileCountDelta` | `3.312 ops/s` | `-` | `-` | `removed` |

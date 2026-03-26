# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-cold,segment-index-get-multisegment-hot`


- Profile: `segment-index-nightly`
- Baseline SHA: `4d49a34ae20bb8595d3ac3657abe6c2cde168e52`
- Candidate SHA: `a87e6e844c0f83bfc93f8ff3748153d6ffec3dbb`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `49.289 ops/s` | `43.070 ops/s` | `-12.62%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `54.579 ops/s` | `40.342 ops/s` | `-26.09%` | `worse` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `165250.784 ops/s` | `165680.149 ops/s` | `+0.26%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `3397504.034 ops/s` | `3777865.175 ops/s` | `+11.20%` | `better` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `89.570 ops/s` | `94.601 ops/s` | `+5.62%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `94.654 ops/s` | `82.090 ops/s` | `-13.27%` | `worse` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `165455.009 ops/s` | `165842.562 ops/s` | `+0.23%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3654469.286 ops/s` | `3671012.090 ops/s` | `+0.45%` | `neutral` |
| `segment-index-get-overlay:getHitAsyncJoin` | `158355.255 ops/s` | `163076.740 ops/s` | `+2.98%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `3857980.743 ops/s` | `3895405.023 ops/s` | `+0.97%` | `neutral` |
| `segment-index-get-overlay:getMissAsyncJoin` | `159058.587 ops/s` | `161397.647 ops/s` | `+1.47%` | `neutral` |
| `segment-index-get-overlay:getMissSync` | `3976859.420 ops/s` | `3730332.937 ops/s` | `-6.20%` | `warning` |
| `segment-index-get-persisted:getHitAsyncJoin` | `59130.213 ops/s` | `61264.844 ops/s` | `+3.61%` | `better` |
| `segment-index-get-persisted:getHitSync` | `114399.001 ops/s` | `109892.906 ops/s` | `-3.94%` | `warning` |
| `segment-index-get-persisted:getMissAsyncJoin` | `159248.012 ops/s` | `166466.175 ops/s` | `+4.53%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3736031.066 ops/s` | `3822535.042 ops/s` | `+2.32%` | `neutral` |
| `segment-index-hot-partition-put:putHotPartition` | `2830113.582 ops/s` | `3068871.333 ops/s` | `+8.44%` | `better` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1591700.032 ops/s` | `1611393.893 ops/s` | `+1.24%` | `neutral` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `305.975 ms/op` | `303.261 ms/op` | `-0.89%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `327.324 ms/op` | `328.243 ms/op` | `+0.28%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `305.405 ms/op` | `303.833 ms/op` | `-0.51%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `511368.830 ops/s` | `562649.495 ops/s` | `+10.03%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `506027.671 ops/s` | `557299.922 ops/s` | `+10.13%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5341.159 ops/s` | `5349.573 ops/s` | `+0.16%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `260868.827 ops/s` | `352124.296 ops/s` | `+34.98%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `259283.967 ops/s` | `265339.151 ops/s` | `+2.34%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1584.860 ops/s` | `86785.145 ops/s` | `+5375.89%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `1945.444 ops/s` | `2044.124 ops/s` | `+5.07%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2204.616 ops/s` | `2202.991 ops/s` | `-0.07%` | `neutral` |
| `segment-index-persisted-mutation:putAsyncJoin` | `1944.697 ops/s` | `2020.736 ops/s` | `+3.91%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2045.278 ops/s` | `2160.229 ops/s` | `+5.62%` | `better` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8539890.654 ops/s` | `8335392.942 ops/s` | `-2.39%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7953638.572 ops/s` | `7846180.623 ops/s` | `-1.35%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `9112200.401 ops/s` | `9031196.790 ops/s` | `-0.89%` | `neutral` |
| `sorted-data-diff-key-read-large:readNextKey` | `6682378.664 ops/s` | `6818517.595 ops/s` | `+2.04%` | `neutral` |

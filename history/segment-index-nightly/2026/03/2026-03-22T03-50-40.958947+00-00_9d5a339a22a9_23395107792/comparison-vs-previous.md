# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-cold,segment-index-get-multisegment-hot`


- Profile: `segment-index-nightly`
- Baseline SHA: `e6adc34214c9ff0a3d8eae146bfacb8b07282732`
- Candidate SHA: `9d5a339a22a9b41105834c8ae9ce3405bf76481c`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `36.394 ops/s` | `42.640 ops/s` | `+17.16%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `39.983 ops/s` | `41.604 ops/s` | `+4.05%` | `better` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `174444.859 ops/s` | `177465.435 ops/s` | `+1.73%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `3459501.892 ops/s` | `3630028.745 ops/s` | `+4.93%` | `better` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `99.120 ops/s` | `88.115 ops/s` | `-11.10%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `101.306 ops/s` | `84.656 ops/s` | `-16.44%` | `worse` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `174956.440 ops/s` | `177113.816 ops/s` | `+1.23%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3647410.147 ops/s` | `3843304.552 ops/s` | `+5.37%` | `better` |
| `segment-index-get-overlay:getHitAsyncJoin` | `170314.575 ops/s` | `174733.512 ops/s` | `+2.59%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `3917583.352 ops/s` | `3986489.829 ops/s` | `+1.76%` | `neutral` |
| `segment-index-get-overlay:getMissAsyncJoin` | `170064.697 ops/s` | `175751.667 ops/s` | `+3.34%` | `better` |
| `segment-index-get-overlay:getMissSync` | `2972302.466 ops/s` | `3809329.274 ops/s` | `+28.16%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `59550.351 ops/s` | `60026.266 ops/s` | `+0.80%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `113588.503 ops/s` | `112084.301 ops/s` | `-1.32%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `173791.160 ops/s` | `176243.737 ops/s` | `+1.41%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `3674595.197 ops/s` | `3711685.984 ops/s` | `+1.01%` | `neutral` |
| `segment-index-hot-partition-put:putHotPartition` | `2687614.649 ops/s` | `2954826.426 ops/s` | `+9.94%` | `better` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1485355.453 ops/s` | `1630376.664 ops/s` | `+9.76%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `312.976 ms/op` | `305.211 ms/op` | `-2.48%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `333.729 ms/op` | `328.706 ms/op` | `-1.51%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `305.646 ms/op` | `303.160 ms/op` | `-0.81%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `500653.792 ops/s` | `544236.222 ops/s` | `+8.71%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `495302.437 ops/s` | `538888.510 ops/s` | `+8.80%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5351.355 ops/s` | `5347.712 ops/s` | `-0.07%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `253990.659 ops/s` | `-` | `-` | `removed` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `252488.865 ops/s` | `-` | `-` | `removed` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1501.794 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `1895.878 ops/s` | `1984.286 ops/s` | `+4.66%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2067.711 ops/s` | `2063.620 ops/s` | `-0.20%` | `neutral` |
| `segment-index-persisted-mutation:putAsyncJoin` | `1793.829 ops/s` | `1986.372 ops/s` | `+10.73%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2054.687 ops/s` | `2129.578 ops/s` | `+3.64%` | `better` |

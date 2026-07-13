# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `1e37c5925410c7e6649b1fa1d4fd33325bcb2d4c`
- Candidate SHA: `8b88b599997497df9b956d4be577a2b7424cedcb`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `1989297.729 ops/s` | `2168067.832 ops/s` | `+8.99%` | `better` |
| `segment-index-get-live:getMissSync` | `2126283.923 ops/s` | `1847837.501 ops/s` | `-13.10%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `2132355.587 ops/s` | `2157199.824 ops/s` | `+1.17%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `1944964.836 ops/s` | `1826254.909 ops/s` | `-6.10%` | `warning` |
| `segment-index-hot-route-put:putHotRoute` | `2074870.872 ops/s` | `2480276.345 ops/s` | `+19.54%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1222769.614 ops/s` | `964559.815 ops/s` | `-21.12%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `308.733 ms/op` | `304.991 ms/op` | `-1.21%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `327.948 ms/op` | `329.013 ms/op` | `+0.32%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `296.175 ms/op` | `300.034 ms/op` | `+1.30%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `449452.376 ops/s` | `452244.094 ops/s` | `+0.62%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `200857.224 ops/s` | `203075.331 ops/s` | `+1.10%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `248595.152 ops/s` | `249168.763 ops/s` | `+0.23%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `862391.276 ops/s` | `873250.554 ops/s` | `+1.26%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `844068.263 ops/s` | `854812.228 ops/s` | `+1.27%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `18323.014 ops/s` | `18438.326 ops/s` | `+0.63%` | `neutral` |

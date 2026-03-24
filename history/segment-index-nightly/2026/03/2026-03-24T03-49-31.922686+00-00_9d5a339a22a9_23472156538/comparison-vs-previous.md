# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `9d5a339a22a9b41105834c8ae9ce3405bf76481c`
- Candidate SHA: `9d5a339a22a9b41105834c8ae9ce3405bf76481c`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `40.003 ops/s` | `41.692 ops/s` | `+4.22%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `40.542 ops/s` | `38.269 ops/s` | `-5.61%` | `warning` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `174362.865 ops/s` | `166240.511 ops/s` | `-4.66%` | `warning` |
| `segment-index-get-multisegment-cold:getMissSync` | `3508647.508 ops/s` | `3596451.671 ops/s` | `+2.50%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `87.507 ops/s` | `99.996 ops/s` | `+14.27%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `90.828 ops/s` | `88.234 ops/s` | `-2.86%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `174199.776 ops/s` | `168318.884 ops/s` | `-3.38%` | `warning` |
| `segment-index-get-multisegment-hot:getMissSync` | `3597809.461 ops/s` | `3791846.513 ops/s` | `+5.39%` | `better` |
| `segment-index-get-overlay:getHitAsyncJoin` | `169377.048 ops/s` | `163736.287 ops/s` | `-3.33%` | `warning` |
| `segment-index-get-overlay:getHitSync` | `4258298.392 ops/s` | `3442088.093 ops/s` | `-19.17%` | `worse` |
| `segment-index-get-overlay:getMissAsyncJoin` | `168994.658 ops/s` | `166602.514 ops/s` | `-1.42%` | `neutral` |
| `segment-index-get-overlay:getMissSync` | `4508556.042 ops/s` | `3847507.806 ops/s` | `-14.66%` | `worse` |
| `segment-index-get-persisted:getHitAsyncJoin` | `61498.761 ops/s` | `60131.280 ops/s` | `-2.22%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `111111.245 ops/s` | `110855.240 ops/s` | `-0.23%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `171467.385 ops/s` | `168671.223 ops/s` | `-1.63%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `3655973.577 ops/s` | `3949723.538 ops/s` | `+8.03%` | `better` |
| `segment-index-hot-partition-put:putHotPartition` | `2964478.679 ops/s` | `3048973.168 ops/s` | `+2.85%` | `neutral` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1700066.708 ops/s` | `1564270.374 ops/s` | `-7.99%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `307.366 ms/op` | `310.171 ms/op` | `+0.91%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `330.870 ms/op` | `333.032 ms/op` | `+0.65%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `304.530 ms/op` | `306.192 ms/op` | `+0.55%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `493395.286 ops/s` | `459809.154 ops/s` | `-6.81%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `488027.157 ops/s` | `454486.716 ops/s` | `-6.87%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5368.129 ops/s` | `5322.438 ops/s` | `-0.85%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `268251.105 ops/s` | `257528.180 ops/s` | `-4.00%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `266839.460 ops/s` | `256006.467 ops/s` | `-4.06%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1411.644 ops/s` | `1521.713 ops/s` | `+7.80%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `1794.957 ops/s` | `2058.830 ops/s` | `+14.70%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `1961.699 ops/s` | `2242.767 ops/s` | `+14.33%` | `better` |
| `segment-index-persisted-mutation:putAsyncJoin` | `1759.673 ops/s` | `1997.053 ops/s` | `+13.49%` | `better` |
| `segment-index-persisted-mutation:putSync` | `1927.228 ops/s` | `2220.967 ops/s` | `+15.24%` | `better` |

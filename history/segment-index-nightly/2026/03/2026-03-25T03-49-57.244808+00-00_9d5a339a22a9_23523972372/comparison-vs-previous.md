# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `9d5a339a22a9b41105834c8ae9ce3405bf76481c`
- Candidate SHA: `9d5a339a22a9b41105834c8ae9ce3405bf76481c`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `41.692 ops/s` | `39.737 ops/s` | `-4.69%` | `warning` |
| `segment-index-get-multisegment-cold:getHitSync` | `38.269 ops/s` | `38.850 ops/s` | `+1.52%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `166240.511 ops/s` | `177615.792 ops/s` | `+6.84%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `3596451.671 ops/s` | `3918819.386 ops/s` | `+8.96%` | `better` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `99.996 ops/s` | `96.134 ops/s` | `-3.86%` | `warning` |
| `segment-index-get-multisegment-hot:getHitSync` | `88.234 ops/s` | `102.547 ops/s` | `+16.22%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `168318.884 ops/s` | `176143.195 ops/s` | `+4.65%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3791846.513 ops/s` | `3701554.044 ops/s` | `-2.38%` | `neutral` |
| `segment-index-get-overlay:getHitAsyncJoin` | `163736.287 ops/s` | `174724.540 ops/s` | `+6.71%` | `better` |
| `segment-index-get-overlay:getHitSync` | `3442088.093 ops/s` | `3922020.623 ops/s` | `+13.94%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `166602.514 ops/s` | `177138.640 ops/s` | `+6.32%` | `better` |
| `segment-index-get-overlay:getMissSync` | `3847507.806 ops/s` | `3651011.937 ops/s` | `-5.11%` | `warning` |
| `segment-index-get-persisted:getHitAsyncJoin` | `60131.280 ops/s` | `61295.950 ops/s` | `+1.94%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `110855.240 ops/s` | `113592.682 ops/s` | `+2.47%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `168671.223 ops/s` | `177254.497 ops/s` | `+5.09%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3949723.538 ops/s` | `3952072.477 ops/s` | `+0.06%` | `neutral` |
| `segment-index-hot-partition-put:putHotPartition` | `3048973.168 ops/s` | `3126250.967 ops/s` | `+2.53%` | `neutral` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1564270.374 ops/s` | `1544999.674 ops/s` | `-1.23%` | `neutral` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `310.171 ms/op` | `306.177 ms/op` | `-1.29%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `333.032 ms/op` | `324.592 ms/op` | `-2.53%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `306.192 ms/op` | `302.642 ms/op` | `-1.16%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `459809.154 ops/s` | `540042.872 ops/s` | `+17.45%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `454486.716 ops/s` | `534700.857 ops/s` | `+17.65%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5322.438 ops/s` | `5342.015 ops/s` | `+0.37%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `257528.180 ops/s` | `327175.663 ops/s` | `+27.04%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `256006.467 ops/s` | `271410.712 ops/s` | `+6.02%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1521.713 ops/s` | `55764.951 ops/s` | `+3564.62%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2058.830 ops/s` | `1542.601 ops/s` | `-25.07%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `2242.767 ops/s` | `1673.027 ops/s` | `-25.40%` | `worse` |
| `segment-index-persisted-mutation:putAsyncJoin` | `1997.053 ops/s` | `1600.315 ops/s` | `-19.87%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `2220.967 ops/s` | `1674.725 ops/s` | `-24.59%` | `worse` |

# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `99f6033423f943fa22655f00429ed039230292dc`
- Candidate SHA: `99f6033423f943fa22655f00429ed039230292dc`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `6281627.959 ops/s` | `4912435.557 ops/s` | `-21.80%` | `worse` |
| `segment-index-get-live:getMissSync` | `6306770.152 ops/s` | `4766358.214 ops/s` | `-24.42%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `373610.592 ops/s` | `275580.970 ops/s` | `-26.24%` | `worse` |
| `segment-index-get-multisegment-cold:getMissSync` | `5695634.390 ops/s` | `3883480.356 ops/s` | `-31.82%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `4323237.072 ops/s` | `3604798.920 ops/s` | `-16.62%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `5949154.641 ops/s` | `4819552.877 ops/s` | `-18.99%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `4251874.605 ops/s` | `3259085.014 ops/s` | `-23.35%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `5716299.038 ops/s` | `4410273.776 ops/s` | `-22.85%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `5127229.782 ops/s` | `4407425.801 ops/s` | `-14.04%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2588903.598 ops/s` | `2201668.780 ops/s` | `-14.96%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `239.447 ms/op` | `263.416 ms/op` | `+10.01%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `257.065 ms/op` | `283.411 ms/op` | `+10.25%` | `better` |
| `segment-index-lifecycle:openExisting` | `236.281 ms/op` | `260.154 ms/op` | `+10.10%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `628212.219 ops/s` | `514049.123 ops/s` | `-18.17%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `338700.659 ops/s` | `263744.232 ops/s` | `-22.13%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `289511.560 ops/s` | `250304.891 ops/s` | `-13.54%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1705311.055 ops/s` | `1142911.956 ops/s` | `-32.98%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1688513.655 ops/s` | `1128360.881 ops/s` | `-33.17%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `16797.399 ops/s` | `14551.076 ops/s` | `-13.37%` | `worse` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `5871.922 ops/s` | `6399.807 ops/s` | `+8.99%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `5898.426 ops/s` | `6401.075 ops/s` | `+8.52%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2445.103 ops/s` | `2230.166 ops/s` | `-8.79%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `2277.100 ops/s` | `2243.061 ops/s` | `-1.49%` | `neutral` |
| `segment-index-range-scan:boundedScan` | `23.046 us/op` | `29.597 us/op` | `+28.43%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1600.335 us/op` | `2056.204 us/op` | `+28.49%` | `better` |
| `segment-index-range-scan:sequentialRead` | `3102.165 us/op` | `3953.275 us/op` | `+27.44%` | `better` |
| `segment-merge-sequential:mergeSequential` | `279.385 us/op` | `331.731 us/op` | `+18.74%` | `better` |

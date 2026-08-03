# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `99f6033423f943fa22655f00429ed039230292dc`
- Candidate SHA: `99f6033423f943fa22655f00429ed039230292dc`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4912435.557 ops/s` | `4550476.178 ops/s` | `-7.37%` | `worse` |
| `segment-index-get-live:getMissSync` | `4766358.214 ops/s` | `4493637.229 ops/s` | `-5.72%` | `warning` |
| `segment-index-get-multisegment-cold:getHitSync` | `275580.970 ops/s` | `268471.416 ops/s` | `-2.58%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `3883480.356 ops/s` | `4250251.350 ops/s` | `+9.44%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `3604798.920 ops/s` | `3691837.769 ops/s` | `+2.41%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `4819552.877 ops/s` | `4561564.906 ops/s` | `-5.35%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `3259085.014 ops/s` | `3250799.949 ops/s` | `-0.25%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `4410273.776 ops/s` | `4840226.371 ops/s` | `+9.75%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `4407425.801 ops/s` | `4175320.125 ops/s` | `-5.27%` | `warning` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2201668.780 ops/s` | `2209100.817 ops/s` | `+0.34%` | `neutral` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `263.416 ms/op` | `259.779 ms/op` | `-1.38%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `283.411 ms/op` | `282.275 ms/op` | `-0.40%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `260.154 ms/op` | `258.025 ms/op` | `-0.82%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `514049.123 ops/s` | `523190.026 ops/s` | `+1.78%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `263744.232 ops/s` | `273832.574 ops/s` | `+3.83%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `250304.891 ops/s` | `249357.452 ops/s` | `-0.38%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1142911.956 ops/s` | `1157683.325 ops/s` | `+1.29%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1128360.881 ops/s` | `1143469.800 ops/s` | `+1.34%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `14551.076 ops/s` | `14213.526 ops/s` | `-2.32%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `6399.807 ops/s` | `6801.607 ops/s` | `+6.28%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `6401.075 ops/s` | `6809.631 ops/s` | `+6.38%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2230.166 ops/s` | `2302.400 ops/s` | `+3.24%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2243.061 ops/s` | `2256.242 ops/s` | `+0.59%` | `neutral` |
| `segment-index-range-scan:boundedScan` | `29.597 us/op` | `30.834 us/op` | `+4.18%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2056.204 us/op` | `2118.212 us/op` | `+3.02%` | `better` |
| `segment-index-range-scan:sequentialRead` | `3953.275 us/op` | `4008.524 us/op` | `+1.40%` | `neutral` |
| `segment-merge-sequential:mergeSequential` | `331.731 us/op` | `334.126 us/op` | `+0.72%` | `neutral` |

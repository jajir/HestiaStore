# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `99f6033423f943fa22655f00429ed039230292dc`
- Candidate SHA: `99f6033423f943fa22655f00429ed039230292dc`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4550476.178 ops/s` | `5256399.071 ops/s` | `+15.51%` | `better` |
| `segment-index-get-live:getMissSync` | `4493637.229 ops/s` | `4156880.564 ops/s` | `-7.49%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `268471.416 ops/s` | `272607.849 ops/s` | `+1.54%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `4250251.350 ops/s` | `4667254.437 ops/s` | `+9.81%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `3691837.769 ops/s` | `3319688.023 ops/s` | `-10.08%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `4561564.906 ops/s` | `4300345.164 ops/s` | `-5.73%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `3250799.949 ops/s` | `3639969.387 ops/s` | `+11.97%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4840226.371 ops/s` | `4651373.856 ops/s` | `-3.90%` | `warning` |
| `segment-index-hot-route-put:putHotRoute` | `4175320.125 ops/s` | `4074274.817 ops/s` | `-2.42%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2209100.817 ops/s` | `2226617.538 ops/s` | `+0.79%` | `neutral` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `259.779 ms/op` | `309.732 ms/op` | `+19.23%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `282.275 ms/op` | `332.062 ms/op` | `+17.64%` | `better` |
| `segment-index-lifecycle:openExisting` | `258.025 ms/op` | `303.755 ms/op` | `+17.72%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `523190.026 ops/s` | `523333.219 ops/s` | `+0.03%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `273832.574 ops/s` | `268060.963 ops/s` | `-2.11%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `249357.452 ops/s` | `255272.256 ops/s` | `+2.37%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1157683.325 ops/s` | `1166581.679 ops/s` | `+0.77%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1143469.800 ops/s` | `1151027.534 ops/s` | `+0.66%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `14213.526 ops/s` | `15554.146 ops/s` | `+9.43%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `6801.607 ops/s` | `7382.096 ops/s` | `+8.53%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `6809.631 ops/s` | `7361.568 ops/s` | `+8.11%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2302.400 ops/s` | `2996.264 ops/s` | `+30.14%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2256.242 ops/s` | `2841.472 ops/s` | `+25.94%` | `better` |
| `segment-index-range-scan:boundedScan` | `30.834 us/op` | `32.040 us/op` | `+3.91%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2118.212 us/op` | `2208.824 us/op` | `+4.28%` | `better` |
| `segment-index-range-scan:sequentialRead` | `4008.524 us/op` | `4251.827 us/op` | `+6.07%` | `better` |
| `segment-merge-sequential:mergeSequential` | `334.126 us/op` | `358.418 us/op` | `+7.27%` | `better` |

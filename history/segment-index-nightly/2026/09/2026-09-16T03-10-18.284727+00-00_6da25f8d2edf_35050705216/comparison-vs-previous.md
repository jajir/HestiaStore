# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4773372.640 ops/s` | `5203433.097 ops/s` | `+9.01%` | `better` |
| `segment-index-get-live:getMissSync` | `4374633.240 ops/s` | `4702526.253 ops/s` | `+7.50%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `274388.392 ops/s` | `284511.093 ops/s` | `+3.69%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `4719740.551 ops/s` | `4105603.269 ops/s` | `-13.01%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `3380032.311 ops/s` | `3681453.859 ops/s` | `+8.92%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `4504597.525 ops/s` | `4907946.521 ops/s` | `+8.95%` | `better` |
| `segment-index-get-persisted:getHitSync` | `2988822.958 ops/s` | `3402017.417 ops/s` | `+13.82%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4237793.522 ops/s` | `4438726.528 ops/s` | `+4.74%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `3814135.584 ops/s` | `4247482.965 ops/s` | `+11.36%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1675490.334 ops/s` | `2384535.304 ops/s` | `+42.32%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `279.352 ms/op` | `245.100 ms/op` | `-12.26%` | `worse` |
| `segment-index-lifecycle:openAndCompact` | `300.634 ms/op` | `262.460 ms/op` | `-12.70%` | `worse` |
| `segment-index-lifecycle:openExisting` | `273.929 ms/op` | `240.373 ms/op` | `-12.25%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `540400.089 ops/s` | `553857.174 ops/s` | `+2.49%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `257805.214 ops/s` | `261202.616 ops/s` | `+1.32%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `282594.874 ops/s` | `292654.558 ops/s` | `+3.56%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1320291.696 ops/s` | `1268361.602 ops/s` | `-3.93%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1300406.869 ops/s` | `1234154.893 ops/s` | `-5.09%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `19884.827 ops/s` | `34206.708 ops/s` | `+72.02%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `7348.541 ops/s` | `7076.578 ops/s` | `-3.70%` | `warning` |
| `segment-index-persisted-mutation-concurrent:putSync` | `7368.481 ops/s` | `7143.815 ops/s` | `-3.05%` | `warning` |
| `segment-index-persisted-mutation:deleteSync` | `3163.317 ops/s` | `2627.484 ops/s` | `-16.94%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `2947.741 ops/s` | `2581.302 ops/s` | `-12.43%` | `worse` |
| `segment-index-range-scan:boundedScan` | `27.998 us/op` | `26.636 us/op` | `-4.87%` | `warning` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2110.742 us/op` | `2000.828 us/op` | `-5.21%` | `warning` |
| `segment-index-range-scan:sequentialRead` | `4209.294 us/op` | `3866.422 us/op` | `-8.15%` | `worse` |
| `segment-merge-sequential:mergeSequential` | `358.761 us/op` | `335.278 us/op` | `-6.55%` | `warning` |

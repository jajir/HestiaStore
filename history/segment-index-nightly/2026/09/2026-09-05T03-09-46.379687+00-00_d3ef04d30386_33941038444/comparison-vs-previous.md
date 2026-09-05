# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Candidate SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4476241.859 ops/s` | `5106492.013 ops/s` | `+14.08%` | `better` |
| `segment-index-get-live:getMissSync` | `4753537.048 ops/s` | `4731752.624 ops/s` | `-0.46%` | `neutral` |
| `segment-index-get-multisegment-cold:getHitSync` | `270753.486 ops/s` | `269781.419 ops/s` | `-0.36%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `4666913.326 ops/s` | `4516443.222 ops/s` | `-3.22%` | `warning` |
| `segment-index-get-multisegment-hot:getHitSync` | `3496801.078 ops/s` | `3693598.525 ops/s` | `+5.63%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3798067.794 ops/s` | `4227964.525 ops/s` | `+11.32%` | `better` |
| `segment-index-get-persisted:getHitSync` | `3627990.776 ops/s` | `3136727.484 ops/s` | `-13.54%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `4238870.652 ops/s` | `4498141.990 ops/s` | `+6.12%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `4011258.669 ops/s` | `4367350.435 ops/s` | `+8.88%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1875489.796 ops/s` | `2192816.654 ops/s` | `+16.92%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `278.767 ms/op` | `244.383 ms/op` | `-12.33%` | `worse` |
| `segment-index-lifecycle:openAndCompact` | `301.502 ms/op` | `268.426 ms/op` | `-10.97%` | `worse` |
| `segment-index-lifecycle:openExisting` | `273.883 ms/op` | `241.435 ms/op` | `-11.85%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `534493.516 ops/s` | `543724.441 ops/s` | `+1.73%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `257143.570 ops/s` | `277276.257 ops/s` | `+7.83%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `277349.946 ops/s` | `266448.184 ops/s` | `-3.93%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1203289.457 ops/s` | `1249994.615 ops/s` | `+3.88%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1183737.738 ops/s` | `1228706.808 ops/s` | `+3.80%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `19551.718 ops/s` | `21287.807 ops/s` | `+8.88%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `7374.336 ops/s` | `6798.669 ops/s` | `-7.81%` | `worse` |
| `segment-index-persisted-mutation-concurrent:putSync` | `7635.497 ops/s` | `6836.646 ops/s` | `-10.46%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `3104.235 ops/s` | `2372.456 ops/s` | `-23.57%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `2935.007 ops/s` | `2297.021 ops/s` | `-21.74%` | `worse` |
| `segment-index-range-scan:boundedScan` | `29.157 us/op` | `31.248 us/op` | `+7.17%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2137.894 us/op` | `1998.880 us/op` | `-6.50%` | `warning` |
| `segment-index-range-scan:sequentialRead` | `4175.662 us/op` | `3876.450 us/op` | `-7.17%` | `worse` |
| `segment-merge-sequential:mergeSequential` | `358.607 us/op` | `331.599 us/op` | `-7.53%` | `worse` |

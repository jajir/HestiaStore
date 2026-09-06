# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Candidate SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `5106492.013 ops/s` | `4744117.365 ops/s` | `-7.10%` | `worse` |
| `segment-index-get-live:getMissSync` | `4731752.624 ops/s` | `4332772.948 ops/s` | `-8.43%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `269781.419 ops/s` | `281119.086 ops/s` | `+4.20%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `4516443.222 ops/s` | `4798410.858 ops/s` | `+6.24%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `3693598.525 ops/s` | `3470591.822 ops/s` | `-6.04%` | `warning` |
| `segment-index-get-multisegment-hot:getMissSync` | `4227964.525 ops/s` | `3999458.579 ops/s` | `-5.40%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `3136727.484 ops/s` | `3389699.220 ops/s` | `+8.06%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4498141.990 ops/s` | `4495156.191 ops/s` | `-0.07%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `4367350.435 ops/s` | `4148394.927 ops/s` | `-5.01%` | `warning` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2192816.654 ops/s` | `2181783.466 ops/s` | `-0.50%` | `neutral` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `244.383 ms/op` | `274.983 ms/op` | `+12.52%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `268.426 ms/op` | `298.517 ms/op` | `+11.21%` | `better` |
| `segment-index-lifecycle:openExisting` | `241.435 ms/op` | `274.824 ms/op` | `+13.83%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `543724.441 ops/s` | `551223.337 ops/s` | `+1.38%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `277276.257 ops/s` | `270246.759 ops/s` | `-2.54%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `266448.184 ops/s` | `280976.578 ops/s` | `+5.45%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1249994.615 ops/s` | `1226758.693 ops/s` | `-1.86%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1228706.808 ops/s` | `1204749.913 ops/s` | `-1.95%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `21287.807 ops/s` | `22008.780 ops/s` | `+3.39%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `6798.669 ops/s` | `7638.271 ops/s` | `+12.35%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `6836.646 ops/s` | `8087.589 ops/s` | `+18.30%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2372.456 ops/s` | `3181.132 ops/s` | `+34.09%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2297.021 ops/s` | `3024.729 ops/s` | `+31.68%` | `better` |
| `segment-index-range-scan:boundedScan` | `31.248 us/op` | `28.665 us/op` | `-8.27%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1998.880 us/op` | `2094.091 us/op` | `+4.76%` | `better` |
| `segment-index-range-scan:sequentialRead` | `3876.450 us/op` | `4092.022 us/op` | `+5.56%` | `better` |
| `segment-merge-sequential:mergeSequential` | `331.599 us/op` | `359.195 us/op` | `+8.32%` | `better` |

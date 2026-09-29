# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `6214201.969 ops/s` | `4896587.860 ops/s` | `-21.20%` | `worse` |
| `segment-index-get-live:getMissSync` | `6625205.130 ops/s` | `4878281.887 ops/s` | `-26.37%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `488562.066 ops/s` | `267162.589 ops/s` | `-45.32%` | `worse` |
| `segment-index-get-multisegment-cold:getMissSync` | `6510517.535 ops/s` | `4071738.635 ops/s` | `-37.46%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `5873137.917 ops/s` | `3667237.194 ops/s` | `-37.56%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `6661607.296 ops/s` | `4529659.457 ops/s` | `-32.00%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `5215623.702 ops/s` | `3355764.485 ops/s` | `-35.66%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `6941114.269 ops/s` | `4521425.595 ops/s` | `-34.86%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `6232241.542 ops/s` | `4356209.180 ops/s` | `-30.10%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `3189485.297 ops/s` | `2304195.196 ops/s` | `-27.76%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `205.192 ms/op` | `246.457 ms/op` | `+20.11%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `218.752 ms/op` | `260.040 ms/op` | `+18.87%` | `better` |
| `segment-index-lifecycle:openExisting` | `208.115 ms/op` | `240.325 ms/op` | `+15.48%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `693697.579 ops/s` | `534409.120 ops/s` | `-22.96%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `413257.097 ops/s` | `240225.055 ops/s` | `-41.87%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `280440.482 ops/s` | `294184.065 ops/s` | `+4.90%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `2144118.640 ops/s` | `1291305.269 ops/s` | `-39.77%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `2095172.050 ops/s` | `1254864.837 ops/s` | `-40.11%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `48946.590 ops/s` | `36440.432 ops/s` | `-25.55%` | `worse` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `7875.240 ops/s` | `8648.055 ops/s` | `+9.81%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `8136.947 ops/s` | `8491.703 ops/s` | `+4.36%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `3509.772 ops/s` | `3245.642 ops/s` | `-7.53%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `3281.935 ops/s` | `3066.086 ops/s` | `-6.58%` | `warning` |
| `segment-index-range-scan:boundedScan` | `17.577 us/op` | `29.062 us/op` | `+65.34%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1170.539 us/op` | `1992.386 us/op` | `+70.21%` | `better` |
| `segment-index-range-scan:sequentialRead` | `2337.527 us/op` | `3908.676 us/op` | `+67.21%` | `better` |
| `segment-merge-sequential:mergeSequential` | `180.747 us/op` | `333.392 us/op` | `+84.45%` | `better` |

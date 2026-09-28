# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4736040.605 ops/s` | `6214201.969 ops/s` | `+31.21%` | `better` |
| `segment-index-get-live:getMissSync` | `4746472.566 ops/s` | `6625205.130 ops/s` | `+39.58%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `273389.728 ops/s` | `488562.066 ops/s` | `+78.71%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `4743346.769 ops/s` | `6510517.535 ops/s` | `+37.26%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `3719666.311 ops/s` | `5873137.917 ops/s` | `+57.89%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `4281136.297 ops/s` | `6661607.296 ops/s` | `+55.60%` | `better` |
| `segment-index-get-persisted:getHitSync` | `3397019.714 ops/s` | `5215623.702 ops/s` | `+53.54%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4654464.615 ops/s` | `6941114.269 ops/s` | `+49.13%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `4259321.561 ops/s` | `6232241.542 ops/s` | `+46.32%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2242340.177 ops/s` | `3189485.297 ops/s` | `+42.24%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `247.452 ms/op` | `205.192 ms/op` | `-17.08%` | `worse` |
| `segment-index-lifecycle:openAndCompact` | `259.475 ms/op` | `218.752 ms/op` | `-15.69%` | `worse` |
| `segment-index-lifecycle:openExisting` | `240.230 ms/op` | `208.115 ms/op` | `-13.37%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `570675.885 ops/s` | `693697.579 ops/s` | `+21.56%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `278919.883 ops/s` | `413257.097 ops/s` | `+48.16%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `291756.002 ops/s` | `280440.482 ops/s` | `-3.88%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1223874.217 ops/s` | `2144118.640 ops/s` | `+75.19%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1188297.555 ops/s` | `2095172.050 ops/s` | `+76.32%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `35576.662 ops/s` | `48946.590 ops/s` | `+37.58%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `6230.169 ops/s` | `7875.240 ops/s` | `+26.40%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `5227.048 ops/s` | `8136.947 ops/s` | `+55.67%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `1963.527 ops/s` | `3509.772 ops/s` | `+78.75%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2231.220 ops/s` | `3281.935 ops/s` | `+47.09%` | `better` |
| `segment-index-range-scan:boundedScan` | `32.262 us/op` | `17.577 us/op` | `-45.52%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1987.824 us/op` | `1170.539 us/op` | `-41.11%` | `worse` |
| `segment-index-range-scan:sequentialRead` | `3883.450 us/op` | `2337.527 us/op` | `-39.81%` | `worse` |
| `segment-merge-sequential:mergeSequential` | `335.076 us/op` | `180.747 us/op` | `-46.06%` | `worse` |

# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `99f6033423f943fa22655f00429ed039230292dc`
- Candidate SHA: `7c9b9709d7b4925f77416d396930aabcca6a6987`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `3112669.116 ops/s` | `5098058.387 ops/s` | `+63.78%` | `better` |
| `segment-index-get-live:getMissSync` | `3001687.960 ops/s` | `4567428.518 ops/s` | `+52.16%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `352021.192 ops/s` | `274447.209 ops/s` | `-22.04%` | `worse` |
| `segment-index-get-multisegment-cold:getMissSync` | `3183234.252 ops/s` | `4451888.907 ops/s` | `+39.85%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `1966855.517 ops/s` | `3575068.512 ops/s` | `+81.77%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3202366.829 ops/s` | `4418239.207 ops/s` | `+37.97%` | `better` |
| `segment-index-get-persisted:getHitSync` | `1829760.414 ops/s` | `3374481.096 ops/s` | `+84.42%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2574652.003 ops/s` | `4724481.118 ops/s` | `+83.50%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2547560.298 ops/s` | `4102574.383 ops/s` | `+61.04%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1492076.264 ops/s` | `2348711.610 ops/s` | `+57.41%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `112.736 ms/op` | `248.498 ms/op` | `+120.42%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `130.411 ms/op` | `271.075 ms/op` | `+107.86%` | `better` |
| `segment-index-lifecycle:openExisting` | `112.100 ms/op` | `241.993 ms/op` | `+115.87%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `533181.715 ops/s` | `529647.596 ops/s` | `-0.66%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `264908.213 ops/s` | `284597.767 ops/s` | `+7.43%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `268273.502 ops/s` | `245049.829 ops/s` | `-8.66%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1209188.224 ops/s` | `1132402.193 ops/s` | `-6.35%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1191016.198 ops/s` | `1117418.936 ops/s` | `-6.18%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `18172.026 ops/s` | `14983.257 ops/s` | `-17.55%` | `worse` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `3881.978 ops/s` | `6910.060 ops/s` | `+78.00%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `4019.710 ops/s` | `6807.998 ops/s` | `+69.37%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `1741.091 ops/s` | `2340.191 ops/s` | `+34.41%` | `better` |
| `segment-index-persisted-mutation:putSync` | `1874.444 ops/s` | `2270.842 ops/s` | `+21.15%` | `better` |
| `segment-index-range-scan:boundedScan` | `20.345 us/op` | `27.479 us/op` | `+35.06%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1606.715 us/op` | `2078.504 us/op` | `+29.36%` | `better` |
| `segment-index-range-scan:sequentialRead` | `2991.268 us/op` | `4218.826 us/op` | `+41.04%` | `better` |
| `segment-merge-sequential:mergeSequential` | `262.976 us/op` | `331.177 us/op` | `+25.93%` | `better` |

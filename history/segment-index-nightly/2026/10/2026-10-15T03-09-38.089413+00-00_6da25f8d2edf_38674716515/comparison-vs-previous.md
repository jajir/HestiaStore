# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `3241540.775 ops/s` | `6141094.298 ops/s` | `+89.45%` | `better` |
| `segment-index-get-live:getMissSync` | `3278716.188 ops/s` | `5757195.500 ops/s` | `+75.59%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `270690.594 ops/s` | `382306.127 ops/s` | `+41.23%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `3373574.993 ops/s` | `5187088.137 ops/s` | `+53.76%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `2551890.339 ops/s` | `4577831.037 ops/s` | `+79.39%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `2805403.407 ops/s` | `6043969.840 ops/s` | `+115.44%` | `better` |
| `segment-index-get-persisted:getHitSync` | `2217430.745 ops/s` | `4623558.684 ops/s` | `+108.51%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3132070.061 ops/s` | `5781838.022 ops/s` | `+84.60%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2918592.872 ops/s` | `5264583.364 ops/s` | `+80.38%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1678980.265 ops/s` | `2528557.760 ops/s` | `+50.60%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `133.140 ms/op` | `214.528 ms/op` | `+61.13%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `149.924 ms/op` | `228.159 ms/op` | `+52.18%` | `better` |
| `segment-index-lifecycle:openExisting` | `130.572 ms/op` | `211.500 ms/op` | `+61.98%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `483958.130 ops/s` | `666040.665 ops/s` | `+37.62%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `225659.424 ops/s` | `343255.615 ops/s` | `+52.11%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `258298.706 ops/s` | `322785.051 ops/s` | `+24.97%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `847238.633 ops/s` | `1539125.919 ops/s` | `+81.66%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `808881.870 ops/s` | `1494526.703 ops/s` | `+84.76%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `38356.763 ops/s` | `44599.216 ops/s` | `+16.27%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `10553.996 ops/s` | `8330.134 ops/s` | `-21.07%` | `worse` |
| `segment-index-persisted-mutation-concurrent:putSync` | `10620.577 ops/s` | `6846.042 ops/s` | `-35.54%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `4330.506 ops/s` | `4018.206 ops/s` | `-7.21%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `4203.195 ops/s` | `3618.242 ops/s` | `-13.92%` | `worse` |
| `segment-index-range-scan:boundedScan` | `28.251 us/op` | `21.416 us/op` | `-24.19%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2036.127 us/op` | `1564.676 us/op` | `-23.15%` | `worse` |
| `segment-index-range-scan:sequentialRead` | `3865.441 us/op` | `2986.678 us/op` | `-22.73%` | `worse` |
| `segment-merge-sequential:mergeSequential` | `331.972 us/op` | `279.217 us/op` | `-15.89%` | `worse` |

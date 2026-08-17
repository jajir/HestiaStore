# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `fe3c86598c2019fd5ff45fef2635519ddcc4ebb3`
- Candidate SHA: `fe3c86598c2019fd5ff45fef2635519ddcc4ebb3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4503847.645 ops/s` | `5996165.903 ops/s` | `+33.13%` | `better` |
| `segment-index-get-live:getMissSync` | `4765500.987 ops/s` | `6247469.006 ops/s` | `+31.10%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `279684.970 ops/s` | `379160.612 ops/s` | `+35.57%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `4303097.162 ops/s` | `5260779.256 ops/s` | `+22.26%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `3631514.856 ops/s` | `4558994.958 ops/s` | `+25.54%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `4503318.619 ops/s` | `5695269.854 ops/s` | `+26.47%` | `better` |
| `segment-index-get-persisted:getHitSync` | `3395544.249 ops/s` | `4416701.636 ops/s` | `+30.07%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4508951.257 ops/s` | `5744636.791 ops/s` | `+27.41%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `4503864.514 ops/s` | `5301161.614 ops/s` | `+17.70%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2246386.558 ops/s` | `2879291.417 ops/s` | `+28.17%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `247.818 ms/op` | `215.203 ms/op` | `-13.16%` | `worse` |
| `segment-index-lifecycle:openAndCompact` | `268.085 ms/op` | `232.848 ms/op` | `-13.14%` | `worse` |
| `segment-index-lifecycle:openExisting` | `242.069 ms/op` | `213.106 ms/op` | `-11.96%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `528578.205 ops/s` | `642646.095 ops/s` | `+21.58%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `277192.340 ops/s` | `350587.808 ops/s` | `+26.48%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `251385.864 ops/s` | `292058.287 ops/s` | `+16.18%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1132218.687 ops/s` | `1726356.282 ops/s` | `+52.48%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1117740.211 ops/s` | `1709346.419 ops/s` | `+52.93%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `14478.476 ops/s` | `17009.863 ops/s` | `+17.48%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `7473.543 ops/s` | `6141.180 ops/s` | `-17.83%` | `worse` |
| `segment-index-persisted-mutation-concurrent:putSync` | `7425.606 ops/s` | `6180.780 ops/s` | `-16.76%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `2507.280 ops/s` | `2601.565 ops/s` | `+3.76%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2432.141 ops/s` | `2522.353 ops/s` | `+3.71%` | `better` |
| `segment-index-range-scan:boundedScan` | `32.529 us/op` | `22.152 us/op` | `-31.90%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2038.984 us/op` | `1549.042 us/op` | `-24.03%` | `worse` |
| `segment-index-range-scan:sequentialRead` | `4048.810 us/op` | `3053.064 us/op` | `-24.59%` | `worse` |
| `segment-merge-sequential:mergeSequential` | `333.018 us/op` | `278.825 us/op` | `-16.27%` | `worse` |

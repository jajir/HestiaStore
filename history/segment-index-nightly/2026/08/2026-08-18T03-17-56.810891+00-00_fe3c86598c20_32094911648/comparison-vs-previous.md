# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `fe3c86598c2019fd5ff45fef2635519ddcc4ebb3`
- Candidate SHA: `fe3c86598c2019fd5ff45fef2635519ddcc4ebb3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `5996165.903 ops/s` | `4674470.695 ops/s` | `-22.04%` | `worse` |
| `segment-index-get-live:getMissSync` | `6247469.006 ops/s` | `4455628.011 ops/s` | `-28.68%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `379160.612 ops/s` | `279079.982 ops/s` | `-26.40%` | `worse` |
| `segment-index-get-multisegment-cold:getMissSync` | `5260779.256 ops/s` | `4778036.069 ops/s` | `-9.18%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `4558994.958 ops/s` | `3227231.839 ops/s` | `-29.21%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `5695269.854 ops/s` | `4007233.025 ops/s` | `-29.64%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `4416701.636 ops/s` | `3031327.524 ops/s` | `-31.37%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `5744636.791 ops/s` | `4186753.993 ops/s` | `-27.12%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `5301161.614 ops/s` | `4012291.659 ops/s` | `-24.31%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2879291.417 ops/s` | `2168597.225 ops/s` | `-24.68%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `215.203 ms/op` | `277.912 ms/op` | `+29.14%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `232.848 ms/op` | `301.716 ms/op` | `+29.58%` | `better` |
| `segment-index-lifecycle:openExisting` | `213.106 ms/op` | `274.955 ms/op` | `+29.02%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `642646.095 ops/s` | `537663.749 ops/s` | `-16.34%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `350587.808 ops/s` | `281336.741 ops/s` | `-19.75%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `292058.287 ops/s` | `256327.008 ops/s` | `-12.23%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1726356.282 ops/s` | `1242239.356 ops/s` | `-28.04%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1709346.419 ops/s` | `1225710.773 ops/s` | `-28.29%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `17009.863 ops/s` | `16528.584 ops/s` | `-2.83%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `6141.180 ops/s` | `8117.824 ops/s` | `+32.19%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `6180.780 ops/s` | `8244.807 ops/s` | `+33.39%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2601.565 ops/s` | `3187.318 ops/s` | `+22.52%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2522.353 ops/s` | `2987.180 ops/s` | `+18.43%` | `better` |
| `segment-index-range-scan:boundedScan` | `22.152 us/op` | `32.218 us/op` | `+45.44%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1549.042 us/op` | `2106.164 us/op` | `+35.97%` | `better` |
| `segment-index-range-scan:sequentialRead` | `3053.064 us/op` | `4179.988 us/op` | `+36.91%` | `better` |
| `segment-merge-sequential:mergeSequential` | `278.825 us/op` | `358.906 us/op` | `+28.72%` | `better` |

# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `5337862.022 ops/s` | `5628159.529 ops/s` | `+5.44%` | `better` |
| `segment-index-get-live:getMissSync` | `4772371.781 ops/s` | `6080145.132 ops/s` | `+27.40%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `270582.966 ops/s` | `363234.550 ops/s` | `+34.24%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `4719982.760 ops/s` | `5406111.817 ops/s` | `+14.54%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `3616509.006 ops/s` | `4716091.183 ops/s` | `+30.40%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `5080867.206 ops/s` | `6231783.633 ops/s` | `+22.65%` | `better` |
| `segment-index-get-persisted:getHitSync` | `3358133.426 ops/s` | `4450419.651 ops/s` | `+32.53%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4274664.867 ops/s` | `5791126.024 ops/s` | `+35.48%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `4352207.202 ops/s` | `5315890.364 ops/s` | `+22.14%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2211468.494 ops/s` | `2601555.719 ops/s` | `+17.64%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `240.406 ms/op` | `215.639 ms/op` | `-10.30%` | `worse` |
| `segment-index-lifecycle:openAndCompact` | `254.486 ms/op` | `228.706 ms/op` | `-10.13%` | `worse` |
| `segment-index-lifecycle:openExisting` | `235.989 ms/op` | `213.025 ms/op` | `-9.73%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `569379.472 ops/s` | `667000.956 ops/s` | `+17.15%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `279100.810 ops/s` | `346831.929 ops/s` | `+24.27%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `290278.661 ops/s` | `320169.027 ops/s` | `+10.30%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1188386.313 ops/s` | `1470441.680 ops/s` | `+23.73%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1152402.954 ops/s` | `1425235.963 ops/s` | `+23.68%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `35983.359 ops/s` | `45205.717 ops/s` | `+25.63%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `8809.452 ops/s` | `6119.680 ops/s` | `-30.53%` | `worse` |
| `segment-index-persisted-mutation-concurrent:putSync` | `8674.661 ops/s` | `5051.687 ops/s` | `-41.77%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `3323.348 ops/s` | `2624.062 ops/s` | `-21.04%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `3223.394 ops/s` | `2804.301 ops/s` | `-13.00%` | `worse` |
| `segment-index-range-scan:boundedScan` | `29.562 us/op` | `22.466 us/op` | `-24.01%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2006.622 us/op` | `1549.176 us/op` | `-22.80%` | `worse` |
| `segment-index-range-scan:sequentialRead` | `3938.225 us/op` | `2991.377 us/op` | `-24.04%` | `worse` |
| `segment-merge-sequential:mergeSequential` | `336.080 us/op` | `279.081 us/op` | `-16.96%` | `worse` |

# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `5628159.529 ops/s` | `4834994.375 ops/s` | `-14.09%` | `worse` |
| `segment-index-get-live:getMissSync` | `6080145.132 ops/s` | `4853339.529 ops/s` | `-20.18%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `363234.550 ops/s` | `281367.275 ops/s` | `-22.54%` | `worse` |
| `segment-index-get-multisegment-cold:getMissSync` | `5406111.817 ops/s` | `4789569.470 ops/s` | `-11.40%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `4716091.183 ops/s` | `3441641.625 ops/s` | `-27.02%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `6231783.633 ops/s` | `4174222.891 ops/s` | `-33.02%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `4450419.651 ops/s` | `3325800.987 ops/s` | `-25.27%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `5791126.024 ops/s` | `4722295.779 ops/s` | `-18.46%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `5315890.364 ops/s` | `3828140.597 ops/s` | `-27.99%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2601555.719 ops/s` | `2252910.200 ops/s` | `-13.40%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `215.639 ms/op` | `244.932 ms/op` | `+13.58%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `228.706 ms/op` | `262.072 ms/op` | `+14.59%` | `better` |
| `segment-index-lifecycle:openExisting` | `213.025 ms/op` | `241.906 ms/op` | `+13.56%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `667000.956 ops/s` | `557205.349 ops/s` | `-16.46%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `346831.929 ops/s` | `269264.914 ops/s` | `-22.36%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `320169.027 ops/s` | `287940.435 ops/s` | `-10.07%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1470441.680 ops/s` | `1135199.227 ops/s` | `-22.80%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1425235.963 ops/s` | `1099703.343 ops/s` | `-22.84%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `45205.717 ops/s` | `35495.884 ops/s` | `-21.48%` | `worse` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `6119.680 ops/s` | `8686.246 ops/s` | `+41.94%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `5051.687 ops/s` | `8282.703 ops/s` | `+63.96%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2624.062 ops/s` | `3018.998 ops/s` | `+15.05%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2804.301 ops/s` | `3169.852 ops/s` | `+13.04%` | `better` |
| `segment-index-range-scan:boundedScan` | `22.466 us/op` | `26.164 us/op` | `+16.46%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1549.176 us/op` | `2064.635 us/op` | `+33.27%` | `better` |
| `segment-index-range-scan:sequentialRead` | `2991.377 us/op` | `3844.410 us/op` | `+28.52%` | `better` |
| `segment-merge-sequential:mergeSequential` | `279.081 us/op` | `333.823 us/op` | `+19.62%` | `better` |

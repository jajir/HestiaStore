# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4704764.547 ops/s` | `2464968.630 ops/s` | `-47.61%` | `worse` |
| `segment-index-get-live:getMissSync` | `4190147.014 ops/s` | `2362538.574 ops/s` | `-43.62%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `285988.413 ops/s` | `300911.015 ops/s` | `+5.22%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `4543952.902 ops/s` | `2813044.046 ops/s` | `-38.09%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `3380371.755 ops/s` | `1893791.393 ops/s` | `-43.98%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `4117454.751 ops/s` | `2794297.067 ops/s` | `-32.14%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `3358859.796 ops/s` | `1765318.273 ops/s` | `-47.44%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `4466231.670 ops/s` | `2758452.507 ops/s` | `-38.24%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `4269996.134 ops/s` | `2648782.571 ops/s` | `-37.97%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2043622.675 ops/s` | `1270985.920 ops/s` | `-37.81%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `227.242 ms/op` | `122.498 ms/op` | `-46.09%` | `worse` |
| `segment-index-lifecycle:openAndCompact` | `243.252 ms/op` | `139.062 ms/op` | `-42.83%` | `worse` |
| `segment-index-lifecycle:openExisting` | `222.846 ms/op` | `119.999 ms/op` | `-46.15%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `557854.607 ops/s` | `487744.912 ops/s` | `-12.57%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `262415.405 ops/s` | `225984.232 ops/s` | `-13.88%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `295439.202 ops/s` | `261760.680 ops/s` | `-11.40%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1181841.660 ops/s` | `917693.593 ops/s` | `-22.35%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1146977.781 ops/s` | `880014.528 ops/s` | `-23.28%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `34863.879 ops/s` | `37679.065 ops/s` | `+8.07%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `8283.575 ops/s` | `5506.743 ops/s` | `-33.52%` | `worse` |
| `segment-index-persisted-mutation-concurrent:putSync` | `8018.906 ops/s` | `5391.356 ops/s` | `-32.77%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `3651.460 ops/s` | `2431.545 ops/s` | `-33.41%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `3628.877 ops/s` | `2407.732 ops/s` | `-33.65%` | `worse` |
| `segment-index-range-scan:boundedScan` | `29.462 us/op` | `24.870 us/op` | `-15.58%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2035.194 us/op` | `1862.209 us/op` | `-8.50%` | `worse` |
| `segment-index-range-scan:sequentialRead` | `3931.545 us/op` | `3555.652 us/op` | `-9.56%` | `worse` |
| `segment-merge-sequential:mergeSequential` | `336.006 us/op` | `312.492 us/op` | `-7.00%` | `warning` |

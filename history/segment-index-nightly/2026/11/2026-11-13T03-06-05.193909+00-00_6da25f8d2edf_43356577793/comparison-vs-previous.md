# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2464968.630 ops/s` | `6172802.003 ops/s` | `+150.42%` | `better` |
| `segment-index-get-live:getMissSync` | `2362538.574 ops/s` | `6810165.513 ops/s` | `+188.26%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `300911.015 ops/s` | `492855.741 ops/s` | `+63.79%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `2813044.046 ops/s` | `6683357.990 ops/s` | `+137.58%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `1893791.393 ops/s` | `5755332.908 ops/s` | `+203.91%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `2794297.067 ops/s` | `6307124.842 ops/s` | `+125.71%` | `better` |
| `segment-index-get-persisted:getHitSync` | `1765318.273 ops/s` | `5355417.439 ops/s` | `+203.37%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2758452.507 ops/s` | `6349015.030 ops/s` | `+130.17%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2648782.571 ops/s` | `6502628.734 ops/s` | `+145.49%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1270985.920 ops/s` | `3118690.872 ops/s` | `+145.38%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `122.498 ms/op` | `182.918 ms/op` | `+49.32%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `139.062 ms/op` | `198.138 ms/op` | `+42.48%` | `better` |
| `segment-index-lifecycle:openExisting` | `119.999 ms/op` | `182.634 ms/op` | `+52.20%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `487744.912 ops/s` | `748029.418 ops/s` | `+53.36%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `225984.232 ops/s` | `459580.487 ops/s` | `+103.37%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `261760.680 ops/s` | `288448.931 ops/s` | `+10.20%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `917693.593 ops/s` | `1913959.189 ops/s` | `+108.56%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `880014.528 ops/s` | `1862872.400 ops/s` | `+111.69%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `37679.065 ops/s` | `51086.789 ops/s` | `+35.58%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `5506.743 ops/s` | `8088.126 ops/s` | `+46.88%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `5391.356 ops/s` | `7986.556 ops/s` | `+48.14%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2431.545 ops/s` | `3832.690 ops/s` | `+57.62%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2407.732 ops/s` | `3683.267 ops/s` | `+52.98%` | `better` |
| `segment-index-range-scan:boundedScan` | `24.870 us/op` | `19.842 us/op` | `-20.22%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1862.209 us/op` | `1190.691 us/op` | `-36.06%` | `worse` |
| `segment-index-range-scan:sequentialRead` | `3555.652 us/op` | `2299.304 us/op` | `-35.33%` | `worse` |
| `segment-merge-sequential:mergeSequential` | `312.492 us/op` | `182.242 us/op` | `-41.68%` | `worse` |

# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Candidate SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2748958.667 ops/s` | `4981897.493 ops/s` | `+81.23%` | `better` |
| `segment-index-get-live:getMissSync` | `2454312.865 ops/s` | `4398723.130 ops/s` | `+79.22%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `306732.132 ops/s` | `277271.895 ops/s` | `-9.60%` | `worse` |
| `segment-index-get-multisegment-cold:getMissSync` | `2628143.990 ops/s` | `4685633.422 ops/s` | `+78.29%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `2043184.123 ops/s` | `3487400.860 ops/s` | `+70.68%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `2817056.245 ops/s` | `4584673.482 ops/s` | `+62.75%` | `better` |
| `segment-index-get-persisted:getHitSync` | `2036844.334 ops/s` | `3416702.626 ops/s` | `+67.74%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2517757.636 ops/s` | `4835896.366 ops/s` | `+92.07%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2348001.880 ops/s` | `4723520.214 ops/s` | `+101.17%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1372498.965 ops/s` | `2172260.635 ops/s` | `+58.27%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `136.043 ms/op` | `246.343 ms/op` | `+81.08%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `155.771 ms/op` | `260.630 ms/op` | `+67.32%` | `better` |
| `segment-index-lifecycle:openExisting` | `132.777 ms/op` | `236.118 ms/op` | `+77.83%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `489995.931 ops/s` | `541797.246 ops/s` | `+10.57%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `218814.417 ops/s` | `270053.396 ops/s` | `+23.42%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `271181.514 ops/s` | `271743.850 ops/s` | `+0.21%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1132759.013 ops/s` | `1231011.763 ops/s` | `+8.67%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1109134.664 ops/s` | `1210684.101 ops/s` | `+9.16%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `23624.348 ops/s` | `20327.662 ops/s` | `-13.95%` | `worse` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `4276.964 ops/s` | `7198.886 ops/s` | `+68.32%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `4670.998 ops/s` | `7181.712 ops/s` | `+53.75%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2091.858 ops/s` | `2399.511 ops/s` | `+14.71%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2133.981 ops/s` | `2370.478 ops/s` | `+11.08%` | `better` |
| `segment-index-range-scan:boundedScan` | `21.566 us/op` | `33.079 us/op` | `+53.39%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1795.523 us/op` | `1955.114 us/op` | `+8.89%` | `better` |
| `segment-index-range-scan:sequentialRead` | `3454.562 us/op` | `3845.188 us/op` | `+11.31%` | `better` |
| `segment-merge-sequential:mergeSequential` | `304.480 us/op` | `334.363 us/op` | `+9.81%` | `better` |

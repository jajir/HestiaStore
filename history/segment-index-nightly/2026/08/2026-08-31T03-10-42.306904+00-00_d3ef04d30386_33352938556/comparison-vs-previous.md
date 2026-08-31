# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Candidate SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4981897.493 ops/s` | `5023657.148 ops/s` | `+0.84%` | `neutral` |
| `segment-index-get-live:getMissSync` | `4398723.130 ops/s` | `4305297.436 ops/s` | `-2.12%` | `neutral` |
| `segment-index-get-multisegment-cold:getHitSync` | `277271.895 ops/s` | `261843.794 ops/s` | `-5.56%` | `warning` |
| `segment-index-get-multisegment-cold:getMissSync` | `4685633.422 ops/s` | `4246049.278 ops/s` | `-9.38%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `3487400.860 ops/s` | `3304150.564 ops/s` | `-5.25%` | `warning` |
| `segment-index-get-multisegment-hot:getMissSync` | `4584673.482 ops/s` | `4569602.691 ops/s` | `-0.33%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `3416702.626 ops/s` | `3145932.929 ops/s` | `-7.92%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `4835896.366 ops/s` | `4469912.064 ops/s` | `-7.57%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `4723520.214 ops/s` | `3985728.614 ops/s` | `-15.62%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2172260.635 ops/s` | `1941320.251 ops/s` | `-10.63%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `246.343 ms/op` | `276.033 ms/op` | `+12.05%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `260.630 ms/op` | `298.731 ms/op` | `+14.62%` | `better` |
| `segment-index-lifecycle:openExisting` | `236.118 ms/op` | `271.967 ms/op` | `+15.18%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `541797.246 ops/s` | `541167.334 ops/s` | `-0.12%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `270053.396 ops/s` | `260645.746 ops/s` | `-3.48%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `271743.850 ops/s` | `280521.588 ops/s` | `+3.23%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1231011.763 ops/s` | `1301123.360 ops/s` | `+5.70%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1210684.101 ops/s` | `1280341.579 ops/s` | `+5.75%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `20327.662 ops/s` | `20781.781 ops/s` | `+2.23%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `7198.886 ops/s` | `8253.277 ops/s` | `+14.65%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `7181.712 ops/s` | `8237.763 ops/s` | `+14.70%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2399.511 ops/s` | `3295.646 ops/s` | `+37.35%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2370.478 ops/s` | `3172.914 ops/s` | `+33.85%` | `better` |
| `segment-index-range-scan:boundedScan` | `33.079 us/op` | `28.910 us/op` | `-12.60%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1955.114 us/op` | `2125.392 us/op` | `+8.71%` | `better` |
| `segment-index-range-scan:sequentialRead` | `3845.188 us/op` | `4027.787 us/op` | `+4.75%` | `better` |
| `segment-merge-sequential:mergeSequential` | `334.363 us/op` | `358.379 us/op` | `+7.18%` | `better` |

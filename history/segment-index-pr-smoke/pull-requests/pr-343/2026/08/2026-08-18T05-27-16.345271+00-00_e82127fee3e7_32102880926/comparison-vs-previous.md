# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `cbbeeb46a13883b23cdb77779edc0adf362acd29`
- Candidate SHA: `e82127fee3e7f4a279e63d9dc92bb12d47fac342`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4962634.512 ops/s` | `5058599.691 ops/s` | `+1.93%` | `neutral` |
| `segment-index-get-live:getMissSync` | `4410397.713 ops/s` | `4535207.305 ops/s` | `+2.83%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `3450966.466 ops/s` | `3686419.090 ops/s` | `+6.82%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `4548327.952 ops/s` | `4714401.250 ops/s` | `+3.65%` | `better` |
| `segment-index-get-persisted:getHitSync` | `3358238.801 ops/s` | `3220181.551 ops/s` | `-4.11%` | `warning` |
| `segment-index-get-persisted:getMissSync` | `4208394.269 ops/s` | `4570320.996 ops/s` | `+8.60%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `3693134.058 ops/s` | `4339994.138 ops/s` | `+17.52%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2011392.790 ops/s` | `2300009.961 ops/s` | `+14.35%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `557020.576 ops/s` | `573470.117 ops/s` | `+2.95%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `367580.999 ops/s` | `398579.155 ops/s` | `+8.43%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `189439.577 ops/s` | `174890.962 ops/s` | `-7.68%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `755786.622 ops/s` | `598652.139 ops/s` | `-20.79%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `737098.437 ops/s` | `581712.954 ops/s` | `-21.08%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `18688.185 ops/s` | `16939.185 ops/s` | `-9.36%` | `worse` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `7300.535 ops/s` | `6944.054 ops/s` | `-4.88%` | `warning` |
| `segment-index-persisted-mutation-concurrent:putSync` | `6441.470 ops/s` | `6937.575 ops/s` | `+7.70%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `3286.977 ops/s` | `2835.057 ops/s` | `-13.75%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `3437.103 ops/s` | `2840.375 ops/s` | `-17.36%` | `worse` |
| `segment-index-range-scan:boundedScan` | `31.424 us/op` | `28.273 us/op` | `-10.03%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2149.381 us/op` | `2078.580 us/op` | `-3.29%` | `warning` |
| `segment-index-range-scan:sequentialRead` | `4181.889 us/op` | `3966.699 us/op` | `-5.15%` | `warning` |
| `segment-merge-sequential:mergeSequential` | `349.544 us/op` | `324.969 us/op` | `-7.03%` | `worse` |

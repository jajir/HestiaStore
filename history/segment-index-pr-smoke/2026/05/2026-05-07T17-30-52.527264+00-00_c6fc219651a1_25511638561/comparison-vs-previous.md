# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `348a4a44a3cd4a5cc0ebc3dabc1e79e31a36628e`
- Candidate SHA: `c6fc219651a1ec4c3f5077c33869c54b9af0720e`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `1874630.676 ops/s` | `2196926.250 ops/s` | `+17.19%` | `better` |
| `segment-index-get-live:getMissSync` | `2397368.890 ops/s` | `3733048.434 ops/s` | `+55.71%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `11879.180 ops/s` | `7110.114 ops/s` | `-40.15%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `2333748.602 ops/s` | `3392512.059 ops/s` | `+45.37%` | `better` |
| `segment-index-get-persisted:getHitSync` | `114103.764 ops/s` | `114582.584 ops/s` | `+0.42%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `2363486.509 ops/s` | `3585836.650 ops/s` | `+51.72%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `1888145.379 ops/s` | `2071669.671 ops/s` | `+9.72%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `989004.445 ops/s` | `1097470.639 ops/s` | `+10.97%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `259705.846 ops/s` | `295360.431 ops/s` | `+13.73%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `112452.819 ops/s` | `124881.577 ops/s` | `+11.05%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `147253.027 ops/s` | `170478.854 ops/s` | `+15.77%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `43369.598 ops/s` | `46321.849 ops/s` | `+6.81%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `38186.644 ops/s` | `41029.747 ops/s` | `+7.45%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `5182.955 ops/s` | `5292.102 ops/s` | `+2.11%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2578.500 ops/s` | `3650.550 ops/s` | `+41.58%` | `better` |
| `segment-index-persisted-mutation:putSync` | `445.070 ops/s` | `462.338 ops/s` | `+3.88%` | `better` |

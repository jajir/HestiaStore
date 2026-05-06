# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `348a4a44a3cd4a5cc0ebc3dabc1e79e31a36628e`
- Candidate SHA: `814ca50a08c2343781cf7285d9961580d6cead0a`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `1874630.676 ops/s` | `2289528.510 ops/s` | `+22.13%` | `better` |
| `segment-index-get-live:getMissSync` | `2397368.890 ops/s` | `3801408.449 ops/s` | `+58.57%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `11879.180 ops/s` | `7016.744 ops/s` | `-40.93%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `2333748.602 ops/s` | `3866321.706 ops/s` | `+65.67%` | `better` |
| `segment-index-get-persisted:getHitSync` | `114103.764 ops/s` | `129541.032 ops/s` | `+13.53%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2363486.509 ops/s` | `3734614.662 ops/s` | `+58.01%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `1888145.379 ops/s` | `2130007.728 ops/s` | `+12.81%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `989004.445 ops/s` | `1101727.615 ops/s` | `+11.40%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `259705.846 ops/s` | `313063.227 ops/s` | `+20.55%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `112452.819 ops/s` | `154222.835 ops/s` | `+37.14%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `147253.027 ops/s` | `158840.392 ops/s` | `+7.87%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `43369.598 ops/s` | `41722.228 ops/s` | `-3.80%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `38186.644 ops/s` | `36470.568 ops/s` | `-4.49%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `5182.955 ops/s` | `5251.661 ops/s` | `+1.33%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2578.500 ops/s` | `1765.833 ops/s` | `-31.52%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `445.070 ops/s` | `427.286 ops/s` | `-4.00%` | `warning` |

# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `c74955bcb66d3a6d2111d59f658e7170ce7bbcdd`
- Candidate SHA: `426a416dff0d4eb226f19656120c53a616057d7e`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `1936521.865 ops/s` | `2402127.926 ops/s` | `+24.04%` | `better` |
| `segment-index-get-live:getMissSync` | `2595227.871 ops/s` | `3422087.699 ops/s` | `+31.86%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `14289.797 ops/s` | `7983.143 ops/s` | `-44.13%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `2504986.114 ops/s` | `3420164.535 ops/s` | `+36.53%` | `better` |
| `segment-index-get-persisted:getHitSync` | `117826.964 ops/s` | `120129.359 ops/s` | `+1.95%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `2585394.426 ops/s` | `3478518.017 ops/s` | `+34.54%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `1900283.396 ops/s` | `1937990.189 ops/s` | `+1.98%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1077686.546 ops/s` | `1060797.299 ops/s` | `-1.57%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `264060.818 ops/s` | `277982.620 ops/s` | `+5.27%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `89038.051 ops/s` | `106667.204 ops/s` | `+19.80%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `175022.767 ops/s` | `171315.416 ops/s` | `-2.12%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `42145.277 ops/s` | `47404.438 ops/s` | `+12.48%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `36858.219 ops/s` | `42071.244 ops/s` | `+14.14%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `5287.058 ops/s` | `5333.194 ops/s` | `+0.87%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `3065.638 ops/s` | `3570.503 ops/s` | `+16.47%` | `better` |
| `segment-index-persisted-mutation:putSync` | `463.525 ops/s` | `461.673 ops/s` | `-0.40%` | `neutral` |

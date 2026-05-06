# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `348a4a44a3cd4a5cc0ebc3dabc1e79e31a36628e`
- Candidate SHA: `c697ec915df1217a91039aef2b0639a573842db6`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `1874630.676 ops/s` | `2298722.184 ops/s` | `+22.62%` | `better` |
| `segment-index-get-live:getMissSync` | `2397368.890 ops/s` | `3704104.374 ops/s` | `+54.51%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `11879.180 ops/s` | `8160.081 ops/s` | `-31.31%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `2333748.602 ops/s` | `3726306.124 ops/s` | `+59.67%` | `better` |
| `segment-index-get-persisted:getHitSync` | `114103.764 ops/s` | `117608.731 ops/s` | `+3.07%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2363486.509 ops/s` | `3464329.576 ops/s` | `+46.58%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `1888145.379 ops/s` | `2003102.512 ops/s` | `+6.09%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `989004.445 ops/s` | `1054519.462 ops/s` | `+6.62%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `259705.846 ops/s` | `288449.307 ops/s` | `+11.07%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `112452.819 ops/s` | `121745.387 ops/s` | `+8.26%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `147253.027 ops/s` | `166703.920 ops/s` | `+13.21%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `43369.598 ops/s` | `48223.588 ops/s` | `+11.19%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `38186.644 ops/s` | `42833.718 ops/s` | `+12.17%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `5182.955 ops/s` | `5389.870 ops/s` | `+3.99%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2578.500 ops/s` | `3514.709 ops/s` | `+36.31%` | `better` |
| `segment-index-persisted-mutation:putSync` | `445.070 ops/s` | `457.606 ops/s` | `+2.82%` | `neutral` |

# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `c74955bcb66d3a6d2111d59f658e7170ce7bbcdd`
- Candidate SHA: `de75606802c8f4d476a30a33d4ebe62c4e68eee1`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `1936521.865 ops/s` | `2177726.945 ops/s` | `+12.46%` | `better` |
| `segment-index-get-live:getMissSync` | `2595227.871 ops/s` | `3614657.693 ops/s` | `+39.28%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `14289.797 ops/s` | `7564.311 ops/s` | `-47.06%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `2504986.114 ops/s` | `3520112.396 ops/s` | `+40.52%` | `better` |
| `segment-index-get-persisted:getHitSync` | `117826.964 ops/s` | `96817.249 ops/s` | `-17.83%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `2585394.426 ops/s` | `3478301.561 ops/s` | `+34.54%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `1900283.396 ops/s` | `1954725.579 ops/s` | `+2.86%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1077686.546 ops/s` | `1039414.153 ops/s` | `-3.55%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `264060.818 ops/s` | `294573.866 ops/s` | `+11.56%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `89038.051 ops/s` | `130088.928 ops/s` | `+46.10%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `175022.767 ops/s` | `164484.938 ops/s` | `-6.02%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `42145.277 ops/s` | `47561.092 ops/s` | `+12.85%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `36858.219 ops/s` | `42252.221 ops/s` | `+14.63%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `5287.058 ops/s` | `5308.871 ops/s` | `+0.41%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `3065.638 ops/s` | `3512.547 ops/s` | `+14.58%` | `better` |
| `segment-index-persisted-mutation:putSync` | `463.525 ops/s` | `454.555 ops/s` | `-1.94%` | `neutral` |

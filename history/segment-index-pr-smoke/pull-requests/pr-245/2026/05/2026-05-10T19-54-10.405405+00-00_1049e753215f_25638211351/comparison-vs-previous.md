# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `de75606802c8f4d476a30a33d4ebe62c4e68eee1`
- Candidate SHA: `1049e753215fe6d653e4b036ed2e60de12b4ffa2`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2177726.945 ops/s` | `2229672.496 ops/s` | `+2.39%` | `neutral` |
| `segment-index-get-live:getMissSync` | `3614657.693 ops/s` | `3927526.876 ops/s` | `+8.66%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `7564.311 ops/s` | `7453.879 ops/s` | `-1.46%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3520112.396 ops/s` | `3640497.641 ops/s` | `+3.42%` | `better` |
| `segment-index-get-persisted:getHitSync` | `96817.249 ops/s` | `118802.667 ops/s` | `+22.71%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3478301.561 ops/s` | `3835625.530 ops/s` | `+10.27%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `1954725.579 ops/s` | `1859504.009 ops/s` | `-4.87%` | `warning` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1039414.153 ops/s` | `1044415.590 ops/s` | `+0.48%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `294573.866 ops/s` | `295768.342 ops/s` | `+0.41%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `130088.928 ops/s` | `142311.507 ops/s` | `+9.40%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `164484.938 ops/s` | `153456.835 ops/s` | `-6.70%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `47561.092 ops/s` | `35648.731 ops/s` | `-25.05%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `42252.221 ops/s` | `30545.775 ops/s` | `-27.71%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `5308.871 ops/s` | `5102.956 ops/s` | `-3.88%` | `warning` |
| `segment-index-persisted-mutation:deleteSync` | `3512.547 ops/s` | `2459.389 ops/s` | `-29.98%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `454.555 ops/s` | `445.733 ops/s` | `-1.94%` | `neutral` |

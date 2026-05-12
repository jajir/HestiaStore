# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `7b4e3e01818862a8152f92756c8f59b19c59d7b3`
- Candidate SHA: `a7cc5948108ad5917dfff700b634f4cb55f4b591`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2253386.281 ops/s` | `2122735.351 ops/s` | `-5.80%` | `warning` |
| `segment-index-get-live:getMissSync` | `3609098.121 ops/s` | `3731316.863 ops/s` | `+3.39%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `7377.704 ops/s` | `7253.533 ops/s` | `-1.68%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3599458.635 ops/s` | `3394113.392 ops/s` | `-5.70%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `107154.357 ops/s` | `2029351.131 ops/s` | `+1793.86%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3585179.422 ops/s` | `3610346.307 ops/s` | `+0.70%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2042678.244 ops/s` | `1916221.323 ops/s` | `-6.19%` | `warning` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1092631.778 ops/s` | `1097236.896 ops/s` | `+0.42%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `301109.608 ops/s` | `324367.486 ops/s` | `+7.72%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `133031.190 ops/s` | `159970.721 ops/s` | `+20.25%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `168078.418 ops/s` | `164396.765 ops/s` | `-2.19%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `47927.725 ops/s` | `46329.047 ops/s` | `-3.34%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `42576.934 ops/s` | `41077.109 ops/s` | `-3.52%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `5350.791 ops/s` | `5251.939 ops/s` | `-1.85%` | `neutral` |

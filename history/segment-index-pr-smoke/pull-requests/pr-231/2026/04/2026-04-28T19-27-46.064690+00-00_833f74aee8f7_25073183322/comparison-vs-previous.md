# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `d071c9602c6022fca98dfbd1e869ca129e4cd557`
- Candidate SHA: `833f74aee8f7123827e5f040b61c8e4d0c98786e`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2146251.189 ops/s` | `2370268.137 ops/s` | `+10.44%` | `better` |
| `segment-index-get-live:getMissSync` | `3623888.505 ops/s` | `3708119.975 ops/s` | `+2.32%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `7590.125 ops/s` | `7382.203 ops/s` | `-2.74%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3557139.759 ops/s` | `3845420.490 ops/s` | `+8.10%` | `better` |
| `segment-index-get-persisted:getHitSync` | `121505.264 ops/s` | `103992.289 ops/s` | `-14.41%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `3532808.625 ops/s` | `3788579.646 ops/s` | `+7.24%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `1965836.839 ops/s` | `2013761.978 ops/s` | `+2.44%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1089192.007 ops/s` | `1043582.099 ops/s` | `-4.19%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `285913.041 ops/s` | `312938.127 ops/s` | `+9.45%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `107818.573 ops/s` | `149809.989 ops/s` | `+38.95%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `178094.468 ops/s` | `163128.138 ops/s` | `-8.40%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `49402.235 ops/s` | `50111.827 ops/s` | `+1.44%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `44096.518 ops/s` | `44785.056 ops/s` | `+1.56%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `5305.717 ops/s` | `5326.771 ops/s` | `+0.40%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `3460.051 ops/s` | `2518.885 ops/s` | `-27.20%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `457.164 ops/s` | `446.019 ops/s` | `-2.44%` | `neutral` |

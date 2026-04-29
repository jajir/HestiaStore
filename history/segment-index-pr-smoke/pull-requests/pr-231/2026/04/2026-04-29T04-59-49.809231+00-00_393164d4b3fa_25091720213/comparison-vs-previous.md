# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `d071c9602c6022fca98dfbd1e869ca129e4cd557`
- Candidate SHA: `393164d4b3fafb0d14dc23200fc74138846f5f8a`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2146251.189 ops/s` | `2388105.846 ops/s` | `+11.27%` | `better` |
| `segment-index-get-live:getMissSync` | `3623888.505 ops/s` | `3735862.545 ops/s` | `+3.09%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `7590.125 ops/s` | `7740.966 ops/s` | `+1.99%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3557139.759 ops/s` | `3658852.939 ops/s` | `+2.86%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `121505.264 ops/s` | `121799.761 ops/s` | `+0.24%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `3532808.625 ops/s` | `4017513.721 ops/s` | `+13.72%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `1965836.839 ops/s` | `1930710.488 ops/s` | `-1.79%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1089192.007 ops/s` | `1127524.239 ops/s` | `+3.52%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `285913.041 ops/s` | `291809.640 ops/s` | `+2.06%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `107818.573 ops/s` | `131727.865 ops/s` | `+22.18%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `178094.468 ops/s` | `160081.775 ops/s` | `-10.11%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `49402.235 ops/s` | `38751.894 ops/s` | `-21.56%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `44096.518 ops/s` | `33470.507 ops/s` | `-24.10%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `5305.717 ops/s` | `5281.387 ops/s` | `-0.46%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `3460.051 ops/s` | `2595.265 ops/s` | `-24.99%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `457.164 ops/s` | `447.901 ops/s` | `-2.03%` | `neutral` |

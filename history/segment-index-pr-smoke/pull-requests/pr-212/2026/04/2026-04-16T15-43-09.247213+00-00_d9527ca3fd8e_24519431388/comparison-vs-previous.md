# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `51724b114025e9af1cae85d0e87d4678c8b87310`
- Candidate SHA: `d9527ca3fd8e4f103e16b71baacc91a4cab49398`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `3654505.325 ops/s` | `3517304.131 ops/s` | `-3.75%` | `warning` |
| `segment-index-get-live:getMissSync` | `4127605.915 ops/s` | `4217138.265 ops/s` | `+2.17%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `94.857 ops/s` | `92.828 ops/s` | `-2.14%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `4019177.695 ops/s` | `3911944.738 ops/s` | `-2.67%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `119895.159 ops/s` | `113731.081 ops/s` | `-5.14%` | `warning` |
| `segment-index-get-persisted:getMissSync` | `4201764.856 ops/s` | `4163737.882 ops/s` | `-0.91%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2644356.758 ops/s` | `2265845.722 ops/s` | `-14.31%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1500639.990 ops/s` | `1507247.962 ops/s` | `+0.44%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `354318.939 ops/s` | `335353.164 ops/s` | `-5.35%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `175579.834 ops/s` | `154558.420 ops/s` | `-11.97%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `178739.104 ops/s` | `180794.745 ops/s` | `+1.15%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `6319.943 ops/s` | `4267.104 ops/s` | `-32.48%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1327.941 ops/s` | `304.731 ops/s` | `-77.05%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `4992.002 ops/s` | `3962.373 ops/s` | `-20.63%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `2476.344 ops/s` | `2662.055 ops/s` | `+7.50%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2222.260 ops/s` | `2542.223 ops/s` | `+14.40%` | `better` |

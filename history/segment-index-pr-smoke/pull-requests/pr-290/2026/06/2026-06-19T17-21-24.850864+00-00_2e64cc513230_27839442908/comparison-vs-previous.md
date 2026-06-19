# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `7cf45898d1c4e656791b1a025e36fee94e8195ca`
- Candidate SHA: `2e64cc51323094161642591ee140039e5d3c5f40`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2194489.707 ops/s` | `2092220.758 ops/s` | `-4.66%` | `warning` |
| `segment-index-get-live:getMissSync` | `1979498.671 ops/s` | `1925244.534 ops/s` | `-2.74%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `13062.426 ops/s` | `-` | `-` | `removed` |
| `segment-index-get-persisted:getHitSync` | `1860351.523 ops/s` | `1754157.734 ops/s` | `-5.71%` | `warning` |
| `segment-index-get-persisted:getMissSync` | `2029938.323 ops/s` | `1944180.760 ops/s` | `-4.22%` | `warning` |
| `segment-index-hot-route-put:putHotRoute` | `2014795.432 ops/s` | `2074960.220 ops/s` | `+2.99%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1079515.033 ops/s` | `1100793.256 ops/s` | `+1.97%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `312289.609 ops/s` | `282814.471 ops/s` | `-9.44%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `160819.154 ops/s` | `119985.284 ops/s` | `-25.39%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `151470.455 ops/s` | `162829.187 ops/s` | `+7.50%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `175158.016 ops/s` | `194359.595 ops/s` | `+10.96%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `159661.687 ops/s` | `178478.082 ops/s` | `+11.79%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `15496.330 ops/s` | `15881.513 ops/s` | `+2.49%` | `neutral` |

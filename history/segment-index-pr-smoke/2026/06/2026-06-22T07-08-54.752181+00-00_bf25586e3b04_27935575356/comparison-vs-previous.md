# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `6fe5c2527770904f67461e3f980a1419927eb51d`
- Candidate SHA: `bf25586e3b048415aef5becf26dc7dd9bcda9866`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2098380.234 ops/s` | `2117777.971 ops/s` | `+0.92%` | `neutral` |
| `segment-index-get-live:getMissSync` | `2012535.676 ops/s` | `2211583.303 ops/s` | `+9.89%` | `better` |
| `segment-index-get-persisted:getHitSync` | `1983643.565 ops/s` | `1721908.130 ops/s` | `-13.19%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `2102502.962 ops/s` | `2041942.713 ops/s` | `-2.88%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2077004.991 ops/s` | `2037872.412 ops/s` | `-1.88%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1086766.375 ops/s` | `1072421.348 ops/s` | `-1.32%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `310571.550 ops/s` | `296120.853 ops/s` | `-4.65%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `138539.692 ops/s` | `117857.966 ops/s` | `-14.93%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `172031.858 ops/s` | `178262.887 ops/s` | `+3.62%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `178563.893 ops/s` | `186272.152 ops/s` | `+4.32%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `162273.644 ops/s` | `171320.515 ops/s` | `+5.58%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `16290.249 ops/s` | `14951.637 ops/s` | `-8.22%` | `worse` |

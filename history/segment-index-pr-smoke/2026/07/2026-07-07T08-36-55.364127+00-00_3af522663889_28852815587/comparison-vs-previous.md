# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `8df8b9848c086730320a4f5a276647c5586602a6`
- Candidate SHA: `3af5226638894ad29c7a620a80e11f651393a855`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2411525.755 ops/s` | `2390797.217 ops/s` | `-0.86%` | `neutral` |
| `segment-index-get-live:getMissSync` | `2208418.920 ops/s` | `2048531.466 ops/s` | `-7.24%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `2052108.169 ops/s` | `2291980.995 ops/s` | `+11.69%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2107155.467 ops/s` | `2439612.049 ops/s` | `+15.78%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2186077.597 ops/s` | `2169886.699 ops/s` | `-0.74%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1104508.205 ops/s` | `1083502.117 ops/s` | `-1.90%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `444661.452 ops/s` | `482898.527 ops/s` | `+8.60%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `276309.247 ops/s` | `325778.455 ops/s` | `+17.90%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `168352.204 ops/s` | `157120.072 ops/s` | `-6.67%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `585781.093 ops/s` | `604920.615 ops/s` | `+3.27%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `571870.361 ops/s` | `589399.839 ops/s` | `+3.07%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `13910.732 ops/s` | `15520.776 ops/s` | `+11.57%` | `better` |

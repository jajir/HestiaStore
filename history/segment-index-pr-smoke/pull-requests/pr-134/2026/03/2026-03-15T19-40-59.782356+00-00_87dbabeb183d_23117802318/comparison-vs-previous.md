# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `87dbabeb183d6e54c9ba22ea677caabc5bf444d3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `169068.481 ops/s` | `+5.57%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `4997402.365 ops/s` | `+5.67%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `53698.720 ops/s` | `-1.57%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `108329.686 ops/s` | `+4.38%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `447157.558 ops/s` | `-0.32%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `441351.472 ops/s` | `-0.32%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `5806.086 ops/s` | `+0.29%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `200758.309 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `197992.340 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `2765.969 ops/s` | `-` | `new` |

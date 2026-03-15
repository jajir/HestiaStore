# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `10641a8137c0333369ea6fdf5a5cf79d15ad35a6`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `165344.524 ops/s` | `+3.24%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `5175535.666 ops/s` | `+9.43%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `58119.974 ops/s` | `+6.53%` | `better` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `107329.743 ops/s` | `+3.42%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `473176.329 ops/s` | `+5.48%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `467397.724 ops/s` | `+5.56%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `5778.606 ops/s` | `-0.18%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `205090.665 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `202200.139 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `2890.526 ops/s` | `-` | `new` |

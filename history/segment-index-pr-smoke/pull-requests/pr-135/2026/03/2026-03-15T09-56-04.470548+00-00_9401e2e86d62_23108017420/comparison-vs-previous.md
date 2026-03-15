# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `9401e2e86d62c1efa3b55b068d2ce847e1f61427`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `250546.823 ops/s` | `+56.44%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `3805509.516 ops/s` | `-19.54%` | `worse` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `75791.631 ops/s` | `+38.92%` | `better` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `111108.669 ops/s` | `+7.06%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `459981.500 ops/s` | `+2.54%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `454287.060 ops/s` | `+2.60%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `5694.440 ops/s` | `-1.64%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `184161.187 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `181488.263 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `2672.923 ops/s` | `-` | `new` |

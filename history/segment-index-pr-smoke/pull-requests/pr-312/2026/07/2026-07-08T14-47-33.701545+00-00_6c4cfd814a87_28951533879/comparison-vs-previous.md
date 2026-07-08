# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `67bbf8e34d2b602923270722df932c97760ccab3`
- Candidate SHA: `6c4cfd814a878127c3eab7125546e2b2fe38b77d`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `1857333.623 ops/s` | `1819508.315 ops/s` | `-2.04%` | `neutral` |
| `segment-index-get-live:getMissSync` | `1756905.838 ops/s` | `1743003.262 ops/s` | `-0.79%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `1756993.207 ops/s` | `1754239.051 ops/s` | `-0.16%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `1790229.895 ops/s` | `1723312.584 ops/s` | `-3.74%` | `warning` |
| `segment-index-hot-route-put:putHotRoute` | `2241915.912 ops/s` | `2287909.506 ops/s` | `+2.05%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1159576.030 ops/s` | `1123301.938 ops/s` | `-3.13%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448655.460 ops/s` | `453524.989 ops/s` | `+1.09%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `250835.861 ops/s` | `256597.912 ops/s` | `+2.30%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `197819.599 ops/s` | `196927.077 ops/s` | `-0.45%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `750849.456 ops/s` | `637678.087 ops/s` | `-15.07%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `731182.845 ops/s` | `618698.451 ops/s` | `-15.38%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `19666.610 ops/s` | `18979.636 ops/s` | `-3.49%` | `warning` |

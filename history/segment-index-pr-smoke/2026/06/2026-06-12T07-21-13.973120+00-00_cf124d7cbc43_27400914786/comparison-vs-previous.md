# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `ec66db06e098be9f665626456c1b5db07b2abb7c`
- Candidate SHA: `cf124d7cbc43ee20f3f96bc435e8d96bdb74fdc7`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `1768613.491 ops/s` | `1783856.938 ops/s` | `+0.86%` | `neutral` |
| `segment-index-get-live:getMissSync` | `1794121.006 ops/s` | `1692473.182 ops/s` | `-5.67%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `1738020.130 ops/s` | `1674102.564 ops/s` | `-3.68%` | `warning` |
| `segment-index-get-persisted:getMissSync` | `1666893.123 ops/s` | `1901620.103 ops/s` | `+14.08%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `1953712.826 ops/s` | `1750482.898 ops/s` | `-10.40%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1116722.484 ops/s` | `1094006.564 ops/s` | `-2.03%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `279872.799 ops/s` | `274598.467 ops/s` | `-1.88%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `134343.036 ops/s` | `130837.444 ops/s` | `-2.61%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `145529.763 ops/s` | `143761.023 ops/s` | `-1.22%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `176620.253 ops/s` | `165658.368 ops/s` | `-6.21%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `162223.496 ops/s` | `148533.604 ops/s` | `-8.44%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `14396.756 ops/s` | `17124.764 ops/s` | `+18.95%` | `better` |

# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `ec66db06e098be9f665626456c1b5db07b2abb7c`
- Candidate SHA: `afa5216abca825480698da8ffc03b64fba59adc8`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2446128.601 ops/s` | `2383735.296 ops/s` | `-2.55%` | `neutral` |
| `segment-index-get-live:getMissSync` | `1965124.751 ops/s` | `2021696.530 ops/s` | `+2.88%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `1800857.924 ops/s` | `1708190.441 ops/s` | `-5.15%` | `warning` |
| `segment-index-get-persisted:getMissSync` | `2108805.379 ops/s` | `2081104.700 ops/s` | `-1.31%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2126175.041 ops/s` | `1858818.864 ops/s` | `-12.57%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1155596.634 ops/s` | `1076358.396 ops/s` | `-6.86%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `290796.975 ops/s` | `297668.957 ops/s` | `+2.36%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `140563.460 ops/s` | `136846.073 ops/s` | `-2.64%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `150233.514 ops/s` | `160822.884 ops/s` | `+7.05%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `165288.988 ops/s` | `172110.286 ops/s` | `+4.13%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `151987.851 ops/s` | `158814.824 ops/s` | `+4.49%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `13301.136 ops/s` | `13295.462 ops/s` | `-0.04%` | `neutral` |

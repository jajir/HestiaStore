# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `ec66db06e098be9f665626456c1b5db07b2abb7c`
- Candidate SHA: `e0f94aa4e67a4fb1f32c90908ad5da1efc9c7757`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2262336.725 ops/s` | `2031057.812 ops/s` | `-10.22%` | `worse` |
| `segment-index-get-live:getMissSync` | `2006223.466 ops/s` | `1999684.168 ops/s` | `-0.33%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `1813602.309 ops/s` | `2058255.993 ops/s` | `+13.49%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2038439.659 ops/s` | `2117488.667 ops/s` | `+3.88%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2119587.684 ops/s` | `1929927.062 ops/s` | `-8.95%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1112033.781 ops/s` | `1151449.674 ops/s` | `+3.54%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `324176.688 ops/s` | `296135.221 ops/s` | `-8.65%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `171297.977 ops/s` | `134471.555 ops/s` | `-21.50%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `152878.711 ops/s` | `161663.666 ops/s` | `+5.75%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `175751.540 ops/s` | `198531.580 ops/s` | `+12.96%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `161437.160 ops/s` | `184119.888 ops/s` | `+14.05%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `14314.381 ops/s` | `14411.692 ops/s` | `+0.68%` | `neutral` |

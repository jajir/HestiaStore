# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `1d013e75d1cb1d4624c28c09ef20598b553fcfb9`
- Candidate SHA: `0111f619ee1a680d02a576aae4cb7203626ab375`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2134449.827 ops/s` | `2181146.605 ops/s` | `+2.19%` | `neutral` |
| `segment-index-get-live:getMissSync` | `2021241.403 ops/s` | `1957880.591 ops/s` | `-3.13%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `2010873.549 ops/s` | `2062833.836 ops/s` | `+2.58%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `2108823.857 ops/s` | `2126124.635 ops/s` | `+0.82%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2219937.860 ops/s` | `2168056.950 ops/s` | `-2.34%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1054141.190 ops/s` | `1106645.823 ops/s` | `+4.98%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `317164.254 ops/s` | `293699.231 ops/s` | `-7.40%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `157145.517 ops/s` | `148467.125 ops/s` | `-5.52%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `160018.736 ops/s` | `145232.107 ops/s` | `-9.24%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `167909.580 ops/s` | `167132.950 ops/s` | `-0.46%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `153046.699 ops/s` | `153158.410 ops/s` | `+0.07%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `14862.881 ops/s` | `13974.540 ops/s` | `-5.98%` | `warning` |

# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `6309468d3d7f8677a76ad087d9c50bde66b3f144`
- Candidate SHA: `1a8a6fd047e95cabf423da796396cebb4035916c`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2399895.721 ops/s` | `2119088.131 ops/s` | `-11.70%` | `worse` |
| `segment-index-get-live:getMissSync` | `2055080.784 ops/s` | `2030797.085 ops/s` | `-1.18%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `2027570.850 ops/s` | `1941259.682 ops/s` | `-4.26%` | `warning` |
| `segment-index-get-persisted:getMissSync` | `1964537.752 ops/s` | `2041932.189 ops/s` | `+3.94%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2128833.618 ops/s` | `2177122.293 ops/s` | `+2.27%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1093516.373 ops/s` | `1110301.258 ops/s` | `+1.53%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `436706.608 ops/s` | `456992.734 ops/s` | `+4.65%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `279815.653 ops/s` | `296467.378 ops/s` | `+5.95%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `156890.955 ops/s` | `160525.356 ops/s` | `+2.32%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `665376.827 ops/s` | `692102.493 ops/s` | `+4.02%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `649228.261 ops/s` | `676230.428 ops/s` | `+4.16%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `16148.566 ops/s` | `15872.064 ops/s` | `-1.71%` | `neutral` |

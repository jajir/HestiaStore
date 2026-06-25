# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `67ee1882a266281258eba5bb0247da374823e228`
- Candidate SHA: `efe7e934705b93964a14c80634f0a9e989e6768c`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2184378.822 ops/s` | `2312969.017 ops/s` | `+5.89%` | `better` |
| `segment-index-get-live:getMissSync` | `1930190.032 ops/s` | `2253594.402 ops/s` | `+16.76%` | `better` |
| `segment-index-get-persisted:getHitSync` | `1849253.807 ops/s` | `1910469.650 ops/s` | `+3.31%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2150369.177 ops/s` | `2318189.228 ops/s` | `+7.80%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `1930702.951 ops/s` | `1998200.099 ops/s` | `+3.50%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1079882.386 ops/s` | `1064071.203 ops/s` | `-1.46%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `285263.096 ops/s` | `309334.725 ops/s` | `+8.44%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `148197.025 ops/s` | `164543.648 ops/s` | `+11.03%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `137066.071 ops/s` | `144791.077 ops/s` | `+5.64%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `155868.317 ops/s` | `193050.871 ops/s` | `+23.86%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `143310.410 ops/s` | `179206.793 ops/s` | `+25.05%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `12557.906 ops/s` | `13844.078 ops/s` | `+10.24%` | `better` |

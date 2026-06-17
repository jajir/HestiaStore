# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `6526d87259948612cba98e2d76f05308f32cccd6`
- Candidate SHA: `04fff6efe8148860f3378fe7e5fa3e45dcdfd7d0`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2794870.103 ops/s` | `2890998.690 ops/s` | `+3.44%` | `better` |
| `segment-index-get-live:getMissSync` | `2541184.424 ops/s` | `2531014.988 ops/s` | `-0.40%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `2525982.213 ops/s` | `2652318.930 ops/s` | `+5.00%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2467195.814 ops/s` | `2596963.506 ops/s` | `+5.26%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2589020.890 ops/s` | `2873835.434 ops/s` | `+11.00%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1440844.992 ops/s` | `1479195.371 ops/s` | `+2.66%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `343231.929 ops/s` | `325081.394 ops/s` | `-5.29%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `125350.417 ops/s` | `107278.666 ops/s` | `-14.42%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `217881.512 ops/s` | `217802.729 ops/s` | `-0.04%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `245824.559 ops/s` | `309142.254 ops/s` | `+25.76%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `225354.917 ops/s` | `290743.133 ops/s` | `+29.02%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `20469.642 ops/s` | `18399.121 ops/s` | `-10.12%` | `worse` |

# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `18a53ed9711d621c676b011285c6999f34435de1`
- Candidate SHA: `1703b98c6930756feb72344e94d1227891bf3fd2`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2020957.388 ops/s` | `2158279.883 ops/s` | `+6.79%` | `better` |
| `segment-index-get-live:getMissSync` | `3864309.996 ops/s` | `3774072.830 ops/s` | `-2.34%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `7371.387 ops/s` | `7715.443 ops/s` | `+4.67%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3986836.082 ops/s` | `3952054.598 ops/s` | `-0.87%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `117039.966 ops/s` | `127285.055 ops/s` | `+8.75%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4226874.775 ops/s` | `3852580.388 ops/s` | `-8.86%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `2156351.504 ops/s` | `2204212.739 ops/s` | `+2.22%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1095391.926 ops/s` | `1128660.802 ops/s` | `+3.04%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `322501.675 ops/s` | `292919.644 ops/s` | `-9.17%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `160611.942 ops/s` | `172654.047 ops/s` | `+7.50%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `161889.733 ops/s` | `120265.597 ops/s` | `-25.71%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `181540.376 ops/s` | `39723.669 ops/s` | `-78.12%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `175313.806 ops/s` | `26229.577 ops/s` | `-85.04%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `6226.570 ops/s` | `13494.092 ops/s` | `+116.72%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2764.270 ops/s` | `2872.863 ops/s` | `+3.93%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2729.913 ops/s` | `2774.597 ops/s` | `+1.64%` | `neutral` |

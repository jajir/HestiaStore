# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `027ee122b625a6c006b7d04ba7284fb8a4e6d9e8`
- Candidate SHA: `e0267401c382ba6c3c6bdc8b5961a42a9a7cef02`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2324536.225 ops/s` | `2627467.797 ops/s` | `+13.03%` | `better` |
| `segment-index-get-live:getMissSync` | `2122772.325 ops/s` | `2302052.962 ops/s` | `+8.45%` | `better` |
| `segment-index-get-persisted:getHitSync` | `2091821.834 ops/s` | `1647061.397 ops/s` | `-21.26%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `2135620.307 ops/s` | `2270471.986 ops/s` | `+6.31%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2096069.522 ops/s` | `2079921.726 ops/s` | `-0.77%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1090661.585 ops/s` | `1101036.916 ops/s` | `+0.95%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `604039.536 ops/s` | `-` | `-` | `removed` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `590692.056 ops/s` | `-` | `-` | `removed` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `13347.480 ops/s` | `-` | `-` | `removed` |

# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `4d7ed0e894ecbc0e65a88805b11ec84b6b680e9c`
- Candidate SHA: `027ee122b625a6c006b7d04ba7284fb8a4e6d9e8`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2154440.208 ops/s` | `2250007.927 ops/s` | `+4.44%` | `better` |
| `segment-index-get-live:getMissSync` | `2008837.922 ops/s` | `1989694.446 ops/s` | `-0.95%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `2018080.732 ops/s` | `1774513.041 ops/s` | `-12.07%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `1948277.014 ops/s` | `2049345.609 ops/s` | `+5.19%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2262979.135 ops/s` | `2149305.412 ops/s` | `-5.02%` | `warning` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1107704.014 ops/s` | `1099170.873 ops/s` | `-0.77%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `488121.069 ops/s` | `-` | `-` | `removed` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `475618.283 ops/s` | `-` | `-` | `removed` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `12502.787 ops/s` | `-` | `-` | `removed` |

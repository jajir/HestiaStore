# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `240bde3d9176c1a1ab1fcd7a76c939364cdded57`
- Candidate SHA: `c4895f0341c322f96aeeeaccd0b67820d220b31c`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2185868.488 ops/s` | `2024996.786 ops/s` | `-7.36%` | `worse` |
| `segment-index-get-live:getMissSync` | `1928169.277 ops/s` | `2014410.546 ops/s` | `+4.47%` | `better` |
| `segment-index-get-persisted:getHitSync` | `1913627.845 ops/s` | `2102553.316 ops/s` | `+9.87%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2106884.743 ops/s` | `2038249.142 ops/s` | `-3.26%` | `warning` |
| `segment-index-hot-route-put:putHotRoute` | `2108330.411 ops/s` | `2096413.667 ops/s` | `-0.57%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1078733.476 ops/s` | `1140038.734 ops/s` | `+5.68%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `327665.778 ops/s` | `303467.653 ops/s` | `-7.39%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `154130.538 ops/s` | `137972.183 ops/s` | `-10.48%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `173535.240 ops/s` | `165495.470 ops/s` | `-4.63%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `180938.939 ops/s` | `194886.559 ops/s` | `+7.71%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `167105.354 ops/s` | `180972.338 ops/s` | `+8.30%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `13833.585 ops/s` | `13914.221 ops/s` | `+0.58%` | `neutral` |

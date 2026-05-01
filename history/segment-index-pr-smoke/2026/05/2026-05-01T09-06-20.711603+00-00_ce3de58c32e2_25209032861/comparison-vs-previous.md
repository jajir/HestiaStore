# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `a974b97c049086030d67fdff8a26871facd1b900`
- Candidate SHA: `ce3de58c32e248c2050b7b04de33b021beecff74`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `1817549.735 ops/s` | `2272152.352 ops/s` | `+25.01%` | `better` |
| `segment-index-get-live:getMissSync` | `2458076.533 ops/s` | `4097711.407 ops/s` | `+66.70%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `13382.539 ops/s` | `7069.352 ops/s` | `-47.17%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `2426854.864 ops/s` | `3875870.149 ops/s` | `+59.71%` | `better` |
| `segment-index-get-persisted:getHitSync` | `107702.059 ops/s` | `113937.625 ops/s` | `+5.79%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2541860.789 ops/s` | `4048573.794 ops/s` | `+59.28%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `1785891.335 ops/s` | `2119487.265 ops/s` | `+18.68%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1012627.018 ops/s` | `1090235.815 ops/s` | `+7.66%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `241874.776 ops/s` | `285621.238 ops/s` | `+18.09%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `94207.814 ops/s` | `112436.739 ops/s` | `+19.35%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `147666.962 ops/s` | `173184.499 ops/s` | `+17.28%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `42472.447 ops/s` | `36996.691 ops/s` | `-12.89%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `37211.513 ops/s` | `31878.016 ops/s` | `-14.33%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `5260.934 ops/s` | `5118.674 ops/s` | `-2.70%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `3541.110 ops/s` | `2655.600 ops/s` | `-25.01%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `441.404 ops/s` | `449.084 ops/s` | `+1.74%` | `neutral` |

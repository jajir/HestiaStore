# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `30f93a9be86df7361bbeb5cf93c3547cbb155d00`
- Candidate SHA: `45c8f1ac8e14d659eb055c1a0dd3437f17b17747`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `3698513.611 ops/s` | `3432906.103 ops/s` | `-7.18%` | `worse` |
| `segment-index-get-live:getMissSync` | `3898567.196 ops/s` | `4359238.178 ops/s` | `+11.82%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `88.322 ops/s` | `87.268 ops/s` | `-1.19%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3767219.777 ops/s` | `3869520.423 ops/s` | `+2.72%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `113703.019 ops/s` | `112063.682 ops/s` | `-1.44%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `3996792.761 ops/s` | `4275441.214 ops/s` | `+6.97%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2338048.490 ops/s` | `2218375.683 ops/s` | `-5.12%` | `warning` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1479583.700 ops/s` | `1356715.248 ops/s` | `-8.30%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `350227.705 ops/s` | `363265.182 ops/s` | `+3.72%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `166014.721 ops/s` | `203963.439 ops/s` | `+22.86%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `184212.984 ops/s` | `159301.743 ops/s` | `-13.52%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `33453.413 ops/s` | `5104.744 ops/s` | `-84.74%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `24974.224 ops/s` | `2010.901 ops/s` | `-91.95%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `8479.189 ops/s` | `3093.843 ops/s` | `-63.51%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `1559.387 ops/s` | `1499.005 ops/s` | `-3.87%` | `warning` |
| `segment-index-persisted-mutation:putSync` | `1499.293 ops/s` | `1467.187 ops/s` | `-2.14%` | `neutral` |

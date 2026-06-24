# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `7cc96a4f02588fc1e87970fc84af8f7132a59154`
- Candidate SHA: `328a14be1eb0e35f0b7ca62914b5f9ecf8e52cd4`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2304196.877 ops/s` | `2151920.049 ops/s` | `-6.61%` | `warning` |
| `segment-index-get-live:getMissSync` | `2252024.371 ops/s` | `2220967.890 ops/s` | `-1.38%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `1859545.977 ops/s` | `1840330.894 ops/s` | `-1.03%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `2273780.953 ops/s` | `2245802.179 ops/s` | `-1.23%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2025930.261 ops/s` | `2032986.304 ops/s` | `+0.35%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1071711.390 ops/s` | `1050848.463 ops/s` | `-1.95%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `307449.873 ops/s` | `311993.981 ops/s` | `+1.48%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `141467.690 ops/s` | `162663.361 ops/s` | `+14.98%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `165982.183 ops/s` | `149330.620 ops/s` | `-10.03%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `155484.010 ops/s` | `179203.934 ops/s` | `+15.26%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `141144.632 ops/s` | `166313.396 ops/s` | `+17.83%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `14339.378 ops/s` | `12890.538 ops/s` | `-10.10%` | `worse` |

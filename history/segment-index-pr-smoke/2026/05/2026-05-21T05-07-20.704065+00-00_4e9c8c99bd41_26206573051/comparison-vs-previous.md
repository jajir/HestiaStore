# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `24a53ff220604e4a2d02e63b54909f2f91ff0ce1`
- Candidate SHA: `4e9c8c99bd41877e6ba6953861eae2752f6f9520`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2179734.786 ops/s` | `2306047.186 ops/s` | `+5.79%` | `better` |
| `segment-index-get-live:getMissSync` | `2005981.019 ops/s` | `2057076.833 ops/s` | `+2.55%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `2067648.834 ops/s` | `1910706.919 ops/s` | `-7.59%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `2165968.314 ops/s` | `2039454.613 ops/s` | `-5.84%` | `warning` |
| `segment-index-hot-route-put:putHotRoute` | `2023150.243 ops/s` | `2103822.559 ops/s` | `+3.99%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1096276.449 ops/s` | `1088509.003 ops/s` | `-0.71%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `283740.293 ops/s` | `289421.802 ops/s` | `+2.00%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `112371.875 ops/s` | `118653.307 ops/s` | `+5.59%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `171368.418 ops/s` | `170768.495 ops/s` | `-0.35%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `171520.441 ops/s` | `152969.194 ops/s` | `-10.82%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `157497.195 ops/s` | `138035.691 ops/s` | `-12.36%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `14023.246 ops/s` | `14933.502 ops/s` | `+6.49%` | `better` |

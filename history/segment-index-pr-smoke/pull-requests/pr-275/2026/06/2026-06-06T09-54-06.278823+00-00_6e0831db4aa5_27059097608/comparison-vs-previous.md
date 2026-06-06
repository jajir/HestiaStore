# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `050681c55cf12c88b63d434fbb3236e80666bd7b`
- Candidate SHA: `6e0831db4aa555317225f65c36d1793dd865475c`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2339753.790 ops/s` | `2239249.071 ops/s` | `-4.30%` | `warning` |
| `segment-index-get-live:getMissSync` | `2055693.383 ops/s` | `2023614.534 ops/s` | `-1.56%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `1803735.833 ops/s` | `1821423.822 ops/s` | `+0.98%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `2118171.983 ops/s` | `2047109.520 ops/s` | `-3.35%` | `warning` |
| `segment-index-hot-route-put:putHotRoute` | `2184187.634 ops/s` | `2011237.935 ops/s` | `-7.92%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1135116.979 ops/s` | `1130281.042 ops/s` | `-0.43%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `292162.384 ops/s` | `307947.450 ops/s` | `+5.40%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `142170.845 ops/s` | `142401.750 ops/s` | `+0.16%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `149991.539 ops/s` | `165545.701 ops/s` | `+10.37%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `194992.147 ops/s` | `179018.680 ops/s` | `-8.19%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `180440.689 ops/s` | `164637.815 ops/s` | `-8.76%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `14551.458 ops/s` | `14380.865 ops/s` | `-1.17%` | `neutral` |

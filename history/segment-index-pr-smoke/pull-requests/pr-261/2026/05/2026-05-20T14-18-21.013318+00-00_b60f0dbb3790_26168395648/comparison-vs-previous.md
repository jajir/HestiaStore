# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `8cbc93641a416f6c24aa4899be7dd94a028fa6f9`
- Candidate SHA: `b60f0dbb37903620f0a4667577864dc8e11193a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2298306.284 ops/s` | `2049729.537 ops/s` | `-10.82%` | `worse` |
| `segment-index-get-live:getMissSync` | `2117638.158 ops/s` | `2006231.493 ops/s` | `-5.26%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `1750297.320 ops/s` | `1820104.782 ops/s` | `+3.99%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2065224.269 ops/s` | `2050014.801 ops/s` | `-0.74%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `1924145.874 ops/s` | `1909395.656 ops/s` | `-0.77%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1023448.704 ops/s` | `1057697.686 ops/s` | `+3.35%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `254607.210 ops/s` | `306163.890 ops/s` | `+20.25%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `108192.248 ops/s` | `125273.408 ops/s` | `+15.79%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `146414.962 ops/s` | `180890.482 ops/s` | `+23.55%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `149603.117 ops/s` | `172501.039 ops/s` | `+15.31%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `131334.646 ops/s` | `156091.861 ops/s` | `+18.85%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `18268.472 ops/s` | `16409.177 ops/s` | `-10.18%` | `worse` |

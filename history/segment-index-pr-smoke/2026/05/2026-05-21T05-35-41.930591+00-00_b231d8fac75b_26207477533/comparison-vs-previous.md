# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `4e9c8c99bd41877e6ba6953861eae2752f6f9520`
- Candidate SHA: `b231d8fac75bf2d77ae759c5fa08f6253e89e553`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2192433.848 ops/s` | `2116083.688 ops/s` | `-3.48%` | `warning` |
| `segment-index-get-live:getMissSync` | `2046027.242 ops/s` | `2078844.622 ops/s` | `+1.60%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `1704689.413 ops/s` | `1842742.936 ops/s` | `+8.10%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2079729.162 ops/s` | `1947470.299 ops/s` | `-6.36%` | `warning` |
| `segment-index-hot-route-put:putHotRoute` | `2056174.113 ops/s` | `2073131.615 ops/s` | `+0.82%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1122342.192 ops/s` | `989115.808 ops/s` | `-11.87%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `269907.272 ops/s` | `258077.093 ops/s` | `-4.38%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `126933.333 ops/s` | `115835.075 ops/s` | `-8.74%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `142973.938 ops/s` | `142242.018 ops/s` | `-0.51%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `164549.774 ops/s` | `174556.987 ops/s` | `+6.08%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `149977.736 ops/s` | `158950.762 ops/s` | `+5.98%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `14572.038 ops/s` | `15606.226 ops/s` | `+7.10%` | `better` |

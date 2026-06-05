# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `30c486013fc8e54c446e74f209a3694799fe6825`
- Candidate SHA: `3296a6f216644b2fe3a03853a5f4ec02d5deb4cd`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2245833.935 ops/s` | `2226340.096 ops/s` | `-0.87%` | `neutral` |
| `segment-index-get-live:getMissSync` | `1880396.286 ops/s` | `2134814.235 ops/s` | `+13.53%` | `better` |
| `segment-index-get-persisted:getHitSync` | `1764094.023 ops/s` | `2045602.787 ops/s` | `+15.96%` | `better` |
| `segment-index-get-persisted:getMissSync` | `1902207.006 ops/s` | `1975305.499 ops/s` | `+3.84%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2070544.243 ops/s` | `2001396.171 ops/s` | `-3.34%` | `warning` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1058651.522 ops/s` | `1116488.047 ops/s` | `+5.46%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `307158.680 ops/s` | `327871.752 ops/s` | `+6.74%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `148196.727 ops/s` | `164910.367 ops/s` | `+11.28%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `158961.952 ops/s` | `162961.384 ops/s` | `+2.52%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `189733.842 ops/s` | `176824.300 ops/s` | `-6.80%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `173375.200 ops/s` | `162336.348 ops/s` | `-6.37%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `16358.643 ops/s` | `14487.952 ops/s` | `-11.44%` | `worse` |

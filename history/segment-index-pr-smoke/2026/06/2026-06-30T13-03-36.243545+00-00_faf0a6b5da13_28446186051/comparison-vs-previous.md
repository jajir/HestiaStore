# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `55de924ccf3e8be908be6dfe7fc2dd8d88d7d938`
- Candidate SHA: `faf0a6b5da1313209f708cc064a9c63e872e18f2`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2142455.421 ops/s` | `2249234.844 ops/s` | `+4.98%` | `better` |
| `segment-index-get-live:getMissSync` | `2310135.613 ops/s` | `2027669.323 ops/s` | `-12.23%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `1826081.035 ops/s` | `1889875.656 ops/s` | `+3.49%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2105640.151 ops/s` | `2231505.568 ops/s` | `+5.98%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2107636.105 ops/s` | `2068900.231 ops/s` | `-1.84%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1067962.540 ops/s` | `1114925.752 ops/s` | `+4.40%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `453534.385 ops/s` | `449250.380 ops/s` | `-0.94%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `271598.543 ops/s` | `273485.611 ops/s` | `+0.69%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `181935.842 ops/s` | `175764.768 ops/s` | `-3.39%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `562177.795 ops/s` | `575421.965 ops/s` | `+2.36%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `547751.146 ops/s` | `560554.523 ops/s` | `+2.34%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `14426.649 ops/s` | `14867.442 ops/s` | `+3.06%` | `better` |

# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `30c486013fc8e54c446e74f209a3694799fe6825`
- Candidate SHA: `1a011cf52739fd4fbe02a79cc778df428acf4e73`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2331630.393 ops/s` | `2305785.190 ops/s` | `-1.11%` | `neutral` |
| `segment-index-get-live:getMissSync` | `1969803.288 ops/s` | `2246321.095 ops/s` | `+14.04%` | `better` |
| `segment-index-get-persisted:getHitSync` | `1765986.970 ops/s` | `1945498.426 ops/s` | `+10.16%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2122434.911 ops/s` | `2341016.881 ops/s` | `+10.30%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `1940165.996 ops/s` | `1874854.164 ops/s` | `-3.37%` | `warning` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1077742.510 ops/s` | `1108937.234 ops/s` | `+2.89%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `287882.462 ops/s` | `291948.107 ops/s` | `+1.41%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `163981.536 ops/s` | `154927.717 ops/s` | `-5.52%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `123900.926 ops/s` | `137020.391 ops/s` | `+10.59%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `165968.631 ops/s` | `160505.825 ops/s` | `-3.29%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `151474.481 ops/s` | `147586.512 ops/s` | `-2.57%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `14494.150 ops/s` | `12919.313 ops/s` | `-10.87%` | `worse` |

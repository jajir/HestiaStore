# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `0aaf66beeb09bb53c3a6668dd930c5175cc1bea9`
- Candidate SHA: `22a358484ccacd047a48ac44106189a8adc50fc0`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2152665.265 ops/s` | `2127128.369 ops/s` | `-1.19%` | `neutral` |
| `segment-index-get-live:getMissSync` | `2063737.431 ops/s` | `1937412.965 ops/s` | `-6.12%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `1756144.433 ops/s` | `1870560.188 ops/s` | `+6.52%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2289417.865 ops/s` | `2200315.325 ops/s` | `-3.89%` | `warning` |
| `segment-index-hot-route-put:putHotRoute` | `2098782.499 ops/s` | `2217846.845 ops/s` | `+5.67%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1114114.003 ops/s` | `1132444.681 ops/s` | `+1.65%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `328361.015 ops/s` | `427687.979 ops/s` | `+30.25%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `157803.771 ops/s` | `255763.180 ops/s` | `+62.08%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `170557.244 ops/s` | `171924.799 ops/s` | `+0.80%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `158427.572 ops/s` | `613442.100 ops/s` | `+287.21%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `144163.495 ops/s` | `598222.855 ops/s` | `+314.96%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `14264.077 ops/s` | `15219.245 ops/s` | `+6.70%` | `better` |

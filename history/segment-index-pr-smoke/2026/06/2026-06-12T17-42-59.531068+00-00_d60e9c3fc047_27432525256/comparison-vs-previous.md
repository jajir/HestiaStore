# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `b9d7389a0e02d0ebf42fe1df6912f5ac7fb1b572`
- Candidate SHA: `d60e9c3fc047d39eb0091b6dbadc07b8dc036ce7`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2239583.107 ops/s` | `2171690.055 ops/s` | `-3.03%` | `warning` |
| `segment-index-get-live:getMissSync` | `1920234.829 ops/s` | `1932347.081 ops/s` | `+0.63%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `1672425.022 ops/s` | `2200364.256 ops/s` | `+31.57%` | `better` |
| `segment-index-get-persisted:getMissSync` | `1936387.137 ops/s` | `2087073.379 ops/s` | `+7.78%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2038008.398 ops/s` | `2102815.328 ops/s` | `+3.18%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1066898.469 ops/s` | `1129093.618 ops/s` | `+5.83%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `288583.935 ops/s` | `272567.465 ops/s` | `-5.55%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `148956.830 ops/s` | `126732.005 ops/s` | `-14.92%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `139627.104 ops/s` | `145835.459 ops/s` | `+4.45%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `167391.993 ops/s` | `165850.018 ops/s` | `-0.92%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `152353.115 ops/s` | `151548.547 ops/s` | `-0.53%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `15038.878 ops/s` | `14301.471 ops/s` | `-4.90%` | `warning` |

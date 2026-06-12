# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `b9d7389a0e02d0ebf42fe1df6912f5ac7fb1b572`
- Candidate SHA: `ad7dae8f0a69ea42bd156bb17b44311fad55f1b0`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `1854430.135 ops/s` | `1804067.551 ops/s` | `-2.72%` | `neutral` |
| `segment-index-get-live:getMissSync` | `1887236.512 ops/s` | `1695715.531 ops/s` | `-10.15%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `1618050.643 ops/s` | `1620904.953 ops/s` | `+0.18%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `1712586.071 ops/s` | `1866495.602 ops/s` | `+8.99%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `1907842.627 ops/s` | `1730329.296 ops/s` | `-9.30%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1105870.201 ops/s` | `1057657.583 ops/s` | `-4.36%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `245037.419 ops/s` | `271180.954 ops/s` | `+10.67%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `111046.358 ops/s` | `119400.792 ops/s` | `+7.52%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `133991.061 ops/s` | `151780.161 ops/s` | `+13.28%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `182042.994 ops/s` | `164733.430 ops/s` | `-9.51%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `168541.993 ops/s` | `150122.352 ops/s` | `-10.93%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `13501.001 ops/s` | `14611.078 ops/s` | `+8.22%` | `better` |

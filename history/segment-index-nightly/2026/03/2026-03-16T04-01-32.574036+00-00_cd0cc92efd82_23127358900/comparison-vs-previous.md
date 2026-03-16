# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `5eac6dab4c182c00e03ffe09d4c5f1cf167b44f7`
- Candidate SHA: `cd0cc92efd820c9eed4f8ca4861418ad2684176a`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-overlay:getHitAsyncJoin` | `160120.740 ops/s` | `169860.520 ops/s` | `+6.08%` | `better` |
| `segment-index-get-overlay:getHitSync` | `5002418.206 ops/s` | `5000214.218 ops/s` | `-0.04%` | `neutral` |
| `segment-index-get-persisted:getHitAsyncJoin` | `59663.615 ops/s` | `59564.880 ops/s` | `-0.17%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `114170.051 ops/s` | `114436.491 ops/s` | `+0.23%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `564811.072 ops/s` | `555852.245 ops/s` | `-1.59%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `558907.592 ops/s` | `549814.636 ops/s` | `-1.63%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5903.480 ops/s` | `6037.609 ops/s` | `+2.27%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `269649.817 ops/s` | `270290.826 ops/s` | `+0.24%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `267943.925 ops/s` | `268711.325 ops/s` | `+0.29%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1705.892 ops/s` | `1579.502 ops/s` | `-7.41%` | `worse` |

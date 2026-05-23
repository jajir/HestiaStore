# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `b231d8fac75bf2d77ae759c5fa08f6253e89e553`
- Candidate SHA: `fb18a694ddde1c481d4bead6cfdaa3d26f8d7638`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2357758.674 ops/s` | `2273593.179 ops/s` | `-3.57%` | `warning` |
| `segment-index-get-live:getMissSync` | `1935271.370 ops/s` | `1954593.101 ops/s` | `+1.00%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `1857093.562 ops/s` | `1629794.021 ops/s` | `-12.24%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `1929111.440 ops/s` | `2090356.254 ops/s` | `+8.36%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2002157.854 ops/s` | `1815108.178 ops/s` | `-9.34%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1063064.395 ops/s` | `1063810.303 ops/s` | `+0.07%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `287874.533 ops/s` | `305598.122 ops/s` | `+6.16%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `127578.144 ops/s` | `139217.981 ops/s` | `+9.12%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `160296.389 ops/s` | `166380.141 ops/s` | `+3.80%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `190960.783 ops/s` | `175559.282 ops/s` | `-8.07%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `176019.475 ops/s` | `159709.627 ops/s` | `-9.27%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `14941.309 ops/s` | `15849.655 ops/s` | `+6.08%` | `better` |

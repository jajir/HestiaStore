# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `67bbf8e34d2b602923270722df932c97760ccab3`
- Candidate SHA: `804a5b52368bbdaf4b0223816e76af66fbce7449`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2513764.882 ops/s` | `2232793.298 ops/s` | `-11.18%` | `worse` |
| `segment-index-get-live:getMissSync` | `1935755.486 ops/s` | `1983266.618 ops/s` | `+2.45%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `1568675.770 ops/s` | `1925387.965 ops/s` | `+22.74%` | `better` |
| `segment-index-get-persisted:getMissSync` | `1891907.170 ops/s` | `2121225.860 ops/s` | `+12.12%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2124570.792 ops/s` | `2091110.182 ops/s` | `-1.57%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1097317.144 ops/s` | `1074442.152 ops/s` | `-2.08%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `464076.338 ops/s` | `434959.654 ops/s` | `-6.27%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `307416.002 ops/s` | `279132.734 ops/s` | `-9.20%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `156660.336 ops/s` | `155826.920 ops/s` | `-0.53%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `565304.912 ops/s` | `623430.687 ops/s` | `+10.28%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `551660.451 ops/s` | `607904.488 ops/s` | `+10.20%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `13644.461 ops/s` | `15526.199 ops/s` | `+13.79%` | `better` |

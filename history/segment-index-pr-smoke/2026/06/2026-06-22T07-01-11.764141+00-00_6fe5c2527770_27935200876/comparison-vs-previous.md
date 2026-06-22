# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `e4db0089603e48f1404346f946cc242f94e6d4ed`
- Candidate SHA: `6fe5c2527770904f67461e3f980a1419927eb51d`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2173299.983 ops/s` | `2305925.049 ops/s` | `+6.10%` | `better` |
| `segment-index-get-live:getMissSync` | `2003873.314 ops/s` | `2012519.322 ops/s` | `+0.43%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `1811915.385 ops/s` | `1850294.009 ops/s` | `+2.12%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `1948362.575 ops/s` | `2010698.800 ops/s` | `+3.20%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2030290.543 ops/s` | `2055795.880 ops/s` | `+1.26%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1038815.087 ops/s` | `1040975.336 ops/s` | `+0.21%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `296875.272 ops/s` | `268797.967 ops/s` | `-9.46%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `151560.576 ops/s` | `109163.348 ops/s` | `-27.97%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `145314.696 ops/s` | `159634.619 ops/s` | `+9.85%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `199568.401 ops/s` | `169153.000 ops/s` | `-15.24%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `185165.979 ops/s` | `156209.275 ops/s` | `-15.64%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `14402.422 ops/s` | `12943.726 ops/s` | `-10.13%` | `worse` |

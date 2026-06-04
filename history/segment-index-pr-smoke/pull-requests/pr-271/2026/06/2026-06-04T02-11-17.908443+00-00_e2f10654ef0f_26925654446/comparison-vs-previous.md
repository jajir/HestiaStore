# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `d5ffeeaaccc81e4004484e8650d5b9d7fd25e529`
- Candidate SHA: `e2f10654ef0ff7092c68bac79e5902aa5e26a2be`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2177969.826 ops/s` | `2230962.123 ops/s` | `+2.43%` | `neutral` |
| `segment-index-get-live:getMissSync` | `2211422.739 ops/s` | `2106330.135 ops/s` | `-4.75%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `1785677.150 ops/s` | `1807073.556 ops/s` | `+1.20%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `2122462.948 ops/s` | `2168854.974 ops/s` | `+2.19%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2138107.652 ops/s` | `1897475.933 ops/s` | `-11.25%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1125224.600 ops/s` | `1172509.710 ops/s` | `+4.20%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `279222.602 ops/s` | `284343.574 ops/s` | `+1.83%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `130102.566 ops/s` | `122856.803 ops/s` | `-5.57%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `149120.035 ops/s` | `161486.771 ops/s` | `+8.29%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `169915.890 ops/s` | `183570.888 ops/s` | `+8.04%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `155667.907 ops/s` | `169009.066 ops/s` | `+8.57%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `14247.983 ops/s` | `14561.822 ops/s` | `+2.20%` | `neutral` |

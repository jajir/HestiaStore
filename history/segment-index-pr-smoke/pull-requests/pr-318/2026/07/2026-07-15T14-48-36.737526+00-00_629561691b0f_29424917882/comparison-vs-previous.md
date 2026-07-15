# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `8b88b599997497df9b956d4be577a2b7424cedcb`
- Candidate SHA: `629561691b0f26f1bde268c021e223a692dac305`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2024460.521 ops/s` | `2155439.816 ops/s` | `+6.47%` | `better` |
| `segment-index-get-live:getMissSync` | `1981829.138 ops/s` | `2028495.179 ops/s` | `+2.35%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `1873527.827 ops/s` | `1755927.031 ops/s` | `-6.28%` | `warning` |
| `segment-index-get-persisted:getMissSync` | `2340918.986 ops/s` | `2110932.711 ops/s` | `-9.82%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `2119693.202 ops/s` | `2059843.917 ops/s` | `-2.82%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1071058.651 ops/s` | `1159981.528 ops/s` | `+8.30%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `452378.215 ops/s` | `449701.867 ops/s` | `-0.59%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `284528.194 ops/s` | `278204.856 ops/s` | `-2.22%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `167850.021 ops/s` | `171497.011 ops/s` | `+2.17%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `512831.322 ops/s` | `613276.865 ops/s` | `+19.59%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `499189.382 ops/s` | `597284.966 ops/s` | `+19.65%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `13641.940 ops/s` | `15991.899 ops/s` | `+17.23%` | `better` |

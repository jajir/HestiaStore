# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `599e3f7187a0fa5414ce9686d524d8b89fc6671d`
- Candidate SHA: `e1bf4a9681c9d755b1168887b359e42761de5a73`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2437281.946 ops/s` | `2460804.315 ops/s` | `+0.97%` | `neutral` |
| `segment-index-get-live:getMissSync` | `1939495.142 ops/s` | `2187965.777 ops/s` | `+12.81%` | `better` |
| `segment-index-get-persisted:getHitSync` | `2136171.242 ops/s` | `2114487.001 ops/s` | `-1.02%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `2131145.205 ops/s` | `2251749.183 ops/s` | `+5.66%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2108440.750 ops/s` | `2186232.084 ops/s` | `+3.69%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1048103.073 ops/s` | `1118781.523 ops/s` | `+6.74%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `428294.412 ops/s` | `420052.306 ops/s` | `-1.92%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `272720.087 ops/s` | `268147.220 ops/s` | `-1.68%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `155574.325 ops/s` | `151905.086 ops/s` | `-2.36%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `593096.146 ops/s` | `691299.747 ops/s` | `+16.56%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `578308.967 ops/s` | `675048.490 ops/s` | `+16.73%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `14787.178 ops/s` | `16251.257 ops/s` | `+9.90%` | `better` |

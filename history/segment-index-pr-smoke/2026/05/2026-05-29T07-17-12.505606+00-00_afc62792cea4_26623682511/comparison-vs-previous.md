# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `cee49a5a0e7ec40a3b8ed557dd1b8b2dc5e22511`
- Candidate SHA: `afc62792cea444b4d3f438b5c4f0ffbce30e8371`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2129495.804 ops/s` | `2159971.964 ops/s` | `+1.43%` | `neutral` |
| `segment-index-get-live:getMissSync` | `2003621.176 ops/s` | `2401209.583 ops/s` | `+19.84%` | `better` |
| `segment-index-get-persisted:getHitSync` | `1876469.369 ops/s` | `1823719.694 ops/s` | `-2.81%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `2196477.520 ops/s` | `1979600.165 ops/s` | `-9.87%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `2185959.686 ops/s` | `2076216.559 ops/s` | `-5.02%` | `warning` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1129785.553 ops/s` | `1096612.400 ops/s` | `-2.94%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `277642.034 ops/s` | `303812.589 ops/s` | `+9.43%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `123528.427 ops/s` | `160394.311 ops/s` | `+29.84%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `154113.607 ops/s` | `143418.278 ops/s` | `-6.94%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `183125.788 ops/s` | `184241.977 ops/s` | `+0.61%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `168288.824 ops/s` | `169598.103 ops/s` | `+0.78%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `14836.963 ops/s` | `14643.874 ops/s` | `-1.30%` | `neutral` |

# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `089a22c9f63c3d154ee6e80eb7f9384e62e15fe5`
- Candidate SHA: `825d249ff4a901a6460ef32f4a00074eb47d28af`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2160911.045 ops/s` | `2251222.520 ops/s` | `+4.18%` | `better` |
| `segment-index-get-live:getMissSync` | `1976062.009 ops/s` | `2152938.250 ops/s` | `+8.95%` | `better` |
| `segment-index-get-persisted:getHitSync` | `1943786.818 ops/s` | `1727580.446 ops/s` | `-11.12%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `2110684.941 ops/s` | `2088350.169 ops/s` | `-1.06%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2088675.206 ops/s` | `2093304.645 ops/s` | `+0.22%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1081395.597 ops/s` | `1098112.014 ops/s` | `+1.55%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `308539.425 ops/s` | `305472.604 ops/s` | `-0.99%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `157905.780 ops/s` | `137815.046 ops/s` | `-12.72%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `150633.645 ops/s` | `167657.559 ops/s` | `+11.30%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `148081.270 ops/s` | `159218.147 ops/s` | `+7.52%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `134292.236 ops/s` | `146386.851 ops/s` | `+9.01%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `13789.034 ops/s` | `12831.296 ops/s` | `-6.95%` | `warning` |

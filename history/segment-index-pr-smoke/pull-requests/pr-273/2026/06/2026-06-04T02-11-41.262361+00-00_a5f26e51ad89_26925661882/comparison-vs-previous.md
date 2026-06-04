# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `d5ffeeaaccc81e4004484e8650d5b9d7fd25e529`
- Candidate SHA: `a5f26e51ad89df47e049dfa58ebb600b885e08ee`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2417966.292 ops/s` | `2341028.319 ops/s` | `-3.18%` | `warning` |
| `segment-index-get-live:getMissSync` | `2058677.205 ops/s` | `1957036.320 ops/s` | `-4.94%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `2063022.052 ops/s` | `1979765.277 ops/s` | `-4.04%` | `warning` |
| `segment-index-get-persisted:getMissSync` | `2120135.253 ops/s` | `2016303.678 ops/s` | `-4.90%` | `warning` |
| `segment-index-hot-route-put:putHotRoute` | `2148305.312 ops/s` | `1914104.038 ops/s` | `-10.90%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1133752.407 ops/s` | `1140769.873 ops/s` | `+0.62%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `309089.112 ops/s` | `293975.122 ops/s` | `-4.89%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `163816.747 ops/s` | `149531.500 ops/s` | `-8.72%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `145272.365 ops/s` | `144443.623 ops/s` | `-0.57%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `192236.838 ops/s` | `170319.380 ops/s` | `-11.40%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `177834.048 ops/s` | `157302.586 ops/s` | `-11.55%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `14402.790 ops/s` | `13016.794 ops/s` | `-9.62%` | `worse` |

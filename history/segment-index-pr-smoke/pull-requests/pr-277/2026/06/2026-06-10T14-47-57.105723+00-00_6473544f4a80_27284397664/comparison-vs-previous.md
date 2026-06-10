# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `ec66db06e098be9f665626456c1b5db07b2abb7c`
- Candidate SHA: `6473544f4a808daea948646226eb67f39258b254`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2267628.704 ops/s` | `2221097.191 ops/s` | `-2.05%` | `neutral` |
| `segment-index-get-live:getMissSync` | `2165279.992 ops/s` | `2245204.659 ops/s` | `+3.69%` | `better` |
| `segment-index-get-persisted:getHitSync` | `1917719.161 ops/s` | `2165621.566 ops/s` | `+12.93%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2014863.719 ops/s` | `2106164.021 ops/s` | `+4.53%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `1964271.845 ops/s` | `2080216.635 ops/s` | `+5.90%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1118869.218 ops/s` | `1089631.761 ops/s` | `-2.61%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `285903.256 ops/s` | `285303.033 ops/s` | `-0.21%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `119880.606 ops/s` | `140038.283 ops/s` | `+16.81%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `166022.650 ops/s` | `145264.750 ops/s` | `-12.50%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `154761.976 ops/s` | `187313.461 ops/s` | `+21.03%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `140267.029 ops/s` | `171968.324 ops/s` | `+22.60%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `14494.946 ops/s` | `15345.137 ops/s` | `+5.87%` | `better` |

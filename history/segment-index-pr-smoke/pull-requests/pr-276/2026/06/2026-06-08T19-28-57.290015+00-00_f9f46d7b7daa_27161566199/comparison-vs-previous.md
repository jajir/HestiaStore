# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `c7587cf224789239262f5a67fbe0c9962510a205`
- Candidate SHA: `f9f46d7b7daa00c6aeda96148ff5e0b733821eca`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2348938.029 ops/s` | `2251317.638 ops/s` | `-4.16%` | `warning` |
| `segment-index-get-live:getMissSync` | `2092699.500 ops/s` | `2076600.863 ops/s` | `-0.77%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `1944234.267 ops/s` | `1850770.507 ops/s` | `-4.81%` | `warning` |
| `segment-index-get-persisted:getMissSync` | `2114958.318 ops/s` | `2121620.255 ops/s` | `+0.31%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `1948337.536 ops/s` | `2212450.456 ops/s` | `+13.56%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1060898.306 ops/s` | `1098225.014 ops/s` | `+3.52%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `302875.504 ops/s` | `320434.072 ops/s` | `+5.80%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `134841.400 ops/s` | `166650.839 ops/s` | `+23.59%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `168034.104 ops/s` | `153783.233 ops/s` | `-8.48%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `190339.997 ops/s` | `174138.380 ops/s` | `-8.51%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `175085.259 ops/s` | `158429.332 ops/s` | `-9.51%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `15254.738 ops/s` | `15709.048 ops/s` | `+2.98%` | `neutral` |

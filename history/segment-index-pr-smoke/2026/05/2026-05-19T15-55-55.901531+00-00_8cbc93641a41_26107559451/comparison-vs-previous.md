# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `2269079a2d2cb8a4ff9d6120a27b647ace492203`
- Candidate SHA: `8cbc93641a416f6c24aa4899be7dd94a028fa6f9`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2295789.524 ops/s` | `2389213.922 ops/s` | `+4.07%` | `better` |
| `segment-index-get-live:getMissSync` | `3907591.766 ops/s` | `2126175.908 ops/s` | `-45.59%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `13363.803 ops/s` | `-` | `-` | `removed` |
| `segment-index-get-multisegment-hot:getMissSync` | `3873557.861 ops/s` | `-` | `-` | `removed` |
| `segment-index-get-persisted:getHitSync` | `2083158.102 ops/s` | `2471681.991 ops/s` | `+18.65%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4127493.676 ops/s` | `2171340.768 ops/s` | `-47.39%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `1774554.956 ops/s` | `1973907.164 ops/s` | `+11.23%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1016907.255 ops/s` | `1056007.250 ops/s` | `+3.84%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `293716.240 ops/s` | `293087.519 ops/s` | `-0.21%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `132048.972 ops/s` | `130775.867 ops/s` | `-0.96%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `161667.268 ops/s` | `162311.652 ops/s` | `+0.40%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `52595.468 ops/s` | `154737.242 ops/s` | `+194.20%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `47190.740 ops/s` | `138793.751 ops/s` | `+194.11%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `5404.728 ops/s` | `15943.490 ops/s` | `+194.99%` | `better` |

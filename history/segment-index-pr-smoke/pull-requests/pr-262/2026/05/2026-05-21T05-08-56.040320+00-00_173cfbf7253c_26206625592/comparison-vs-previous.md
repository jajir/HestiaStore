# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `4e9c8c99bd41877e6ba6953861eae2752f6f9520`
- Candidate SHA: `173cfbf7253c72703c6f9599f0e3ce10b9afc0a2`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2602978.775 ops/s` | `2741230.043 ops/s` | `+5.31%` | `better` |
| `segment-index-get-live:getMissSync` | `2514626.547 ops/s` | `2480620.132 ops/s` | `-1.35%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `2558664.033 ops/s` | `-` | `-` | `removed` |
| `segment-index-get-persisted:getHitSync` | `2289882.251 ops/s` | `2554789.367 ops/s` | `+11.57%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2606955.479 ops/s` | `2566722.153 ops/s` | `-1.54%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2654784.200 ops/s` | `2671538.371 ops/s` | `+0.63%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1382564.618 ops/s` | `1447756.891 ops/s` | `+4.72%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `345035.060 ops/s` | `346803.403 ops/s` | `+0.51%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `126393.072 ops/s` | `121869.348 ops/s` | `-3.58%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `218641.989 ops/s` | `224934.054 ops/s` | `+2.88%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `278276.597 ops/s` | `297034.136 ops/s` | `+6.74%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `258996.682 ops/s` | `279728.131 ops/s` | `+8.00%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `19279.915 ops/s` | `17306.005 ops/s` | `-10.24%` | `worse` |

# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `dce1fbc911dbad09d8df685697a7c8b43069ed09`
- Candidate SHA: `fc9bba4bc1d3070810e710d992e4c9fbe9de8c6b`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2407073.345 ops/s` | `2366922.899 ops/s` | `-1.67%` | `neutral` |
| `segment-index-get-live:getMissSync` | `2163580.999 ops/s` | `1989038.909 ops/s` | `-8.07%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `1778905.652 ops/s` | `1936949.561 ops/s` | `+8.88%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2164287.462 ops/s` | `2113418.389 ops/s` | `-2.35%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2138456.437 ops/s` | `2166873.859 ops/s` | `+1.33%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1078468.800 ops/s` | `1082059.443 ops/s` | `+0.33%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `414942.190 ops/s` | `467147.096 ops/s` | `+12.58%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `244819.960 ops/s` | `313516.693 ops/s` | `+28.06%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `170122.230 ops/s` | `153630.403 ops/s` | `-9.69%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `598444.911 ops/s` | `554938.088 ops/s` | `-7.27%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `584518.476 ops/s` | `540542.715 ops/s` | `-7.52%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `13926.435 ops/s` | `14395.374 ops/s` | `+3.37%` | `better` |

# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `22a358484ccacd047a48ac44106189a8adc50fc0`
- Candidate SHA: `ba315c9b6d99e9ef021ee4ca3f5ac8b78827fd14`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2270547.858 ops/s` | `2126556.520 ops/s` | `-6.34%` | `warning` |
| `segment-index-get-live:getMissSync` | `2089371.653 ops/s` | `1985066.599 ops/s` | `-4.99%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `1879394.068 ops/s` | `2128201.827 ops/s` | `+13.24%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2047190.202 ops/s` | `2110168.877 ops/s` | `+3.08%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2175953.480 ops/s` | `2193808.720 ops/s` | `+0.82%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1067376.916 ops/s` | `1129427.351 ops/s` | `+5.81%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `435330.787 ops/s` | `414039.150 ops/s` | `-4.89%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `290364.076 ops/s` | `234392.175 ops/s` | `-19.28%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `144966.710 ops/s` | `179646.975 ops/s` | `+23.92%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `662987.896 ops/s` | `510132.586 ops/s` | `-23.06%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `646899.248 ops/s` | `496449.051 ops/s` | `-23.26%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `16088.647 ops/s` | `13683.534 ops/s` | `-14.95%` | `worse` |

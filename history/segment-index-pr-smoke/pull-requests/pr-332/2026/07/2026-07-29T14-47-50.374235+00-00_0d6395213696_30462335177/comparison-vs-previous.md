# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `96593cd49b374380016362f1fc4d5bacdf709832`
- Candidate SHA: `0d63952136969c83db8c5907ab7a94d3b55802b3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2128444.564 ops/s` | `2326480.494 ops/s` | `+9.30%` | `better` |
| `segment-index-get-live:getMissSync` | `2299151.066 ops/s` | `2211125.151 ops/s` | `-3.83%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `1811284.537 ops/s` | `1898631.520 ops/s` | `+4.82%` | `better` |
| `segment-index-get-persisted:getMissSync` | `1981511.963 ops/s` | `1982362.201 ops/s` | `+0.04%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2083978.235 ops/s` | `2175887.227 ops/s` | `+4.41%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1117017.169 ops/s` | `1081614.202 ops/s` | `-3.17%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `429518.838 ops/s` | `446261.192 ops/s` | `+3.90%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `266881.734 ops/s` | `284105.965 ops/s` | `+6.45%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `162637.103 ops/s` | `162155.227 ops/s` | `-0.30%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `560663.125 ops/s` | `596335.873 ops/s` | `+6.36%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `547008.757 ops/s` | `582570.720 ops/s` | `+6.50%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `13654.367 ops/s` | `13765.152 ops/s` | `+0.81%` | `neutral` |

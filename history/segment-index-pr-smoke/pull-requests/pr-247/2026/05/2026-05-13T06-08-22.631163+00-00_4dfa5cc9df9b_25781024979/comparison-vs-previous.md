# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `7b4e3e01818862a8152f92756c8f59b19c59d7b3`
- Candidate SHA: `4dfa5cc9df9bcd656ce0059596a15c89c1ac4f25`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2189207.467 ops/s` | `2124680.414 ops/s` | `-2.95%` | `neutral` |
| `segment-index-get-live:getMissSync` | `3566491.340 ops/s` | `3465336.833 ops/s` | `-2.84%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `7593.050 ops/s` | `7582.086 ops/s` | `-0.14%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3609180.914 ops/s` | `3538249.170 ops/s` | `-1.97%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `115566.048 ops/s` | `1817492.879 ops/s` | `+1472.69%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3424870.227 ops/s` | `3634506.623 ops/s` | `+6.12%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2125038.443 ops/s` | `1946409.892 ops/s` | `-8.41%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1079902.750 ops/s` | `1035769.719 ops/s` | `-4.09%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `289899.314 ops/s` | `301517.200 ops/s` | `+4.01%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `120170.611 ops/s` | `131747.628 ops/s` | `+9.63%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `169728.703 ops/s` | `169769.573 ops/s` | `+0.02%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `42769.635 ops/s` | `37209.566 ops/s` | `-13.00%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `37560.901 ops/s` | `31907.226 ops/s` | `-15.05%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `5208.734 ops/s` | `5302.341 ops/s` | `+1.80%` | `neutral` |

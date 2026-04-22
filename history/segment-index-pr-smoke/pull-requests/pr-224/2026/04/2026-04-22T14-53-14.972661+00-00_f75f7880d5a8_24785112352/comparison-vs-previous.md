# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `18a53ed9711d621c676b011285c6999f34435de1`
- Candidate SHA: `f75f7880d5a8f854a94f9a667a943b26116e52d4`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2018663.454 ops/s` | `2000368.113 ops/s` | `-0.91%` | `neutral` |
| `segment-index-get-live:getMissSync` | `3592703.004 ops/s` | `3719188.030 ops/s` | `+3.52%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `7573.907 ops/s` | `7336.095 ops/s` | `-3.14%` | `warning` |
| `segment-index-get-multisegment-hot:getMissSync` | `3881869.209 ops/s` | `3770846.010 ops/s` | `-2.86%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `121959.671 ops/s` | `109037.341 ops/s` | `-10.60%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `3748339.701 ops/s` | `3750266.060 ops/s` | `+0.05%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2200370.794 ops/s` | `2173781.914 ops/s` | `-1.21%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1094035.450 ops/s` | `1123372.500 ops/s` | `+2.68%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `307023.456 ops/s` | `318153.525 ops/s` | `+3.63%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `140709.540 ops/s` | `146756.064 ops/s` | `+4.30%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `166313.915 ops/s` | `171397.462 ops/s` | `+3.06%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `91885.119 ops/s` | `27491.989 ops/s` | `-70.08%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `83980.325 ops/s` | `11486.970 ops/s` | `-86.32%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `7904.794 ops/s` | `16005.019 ops/s` | `+102.47%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2435.195 ops/s` | `2438.529 ops/s` | `+0.14%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `2404.937 ops/s` | `2331.061 ops/s` | `-3.07%` | `warning` |

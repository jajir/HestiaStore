# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `30f93a9be86df7361bbeb5cf93c3547cbb155d00`
- Candidate SHA: `e06ef3dd1a496aef1791c4597444ac4968c35b1d`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `3615599.932 ops/s` | `3764567.012 ops/s` | `+4.12%` | `better` |
| `segment-index-get-live:getMissSync` | `4127350.319 ops/s` | `4138192.678 ops/s` | `+0.26%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `87.426 ops/s` | `74.397 ops/s` | `-14.90%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `3772544.019 ops/s` | `3976266.463 ops/s` | `+5.40%` | `better` |
| `segment-index-get-persisted:getHitSync` | `121408.000 ops/s` | `126485.750 ops/s` | `+4.18%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4389580.865 ops/s` | `4229948.990 ops/s` | `-3.64%` | `warning` |
| `segment-index-hot-route-put:putHotRoute` | `2242289.203 ops/s` | `2300255.431 ops/s` | `+2.59%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1539815.109 ops/s` | `1491578.300 ops/s` | `-3.13%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `353880.854 ops/s` | `349876.380 ops/s` | `-1.13%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `196156.863 ops/s` | `166122.683 ops/s` | `-15.31%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `157723.992 ops/s` | `183753.698 ops/s` | `+16.50%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `35479.866 ops/s` | `152777.377 ops/s` | `+330.60%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `26706.558 ops/s` | `523.886 ops/s` | `-98.04%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `8773.308 ops/s` | `152253.490 ops/s` | `+1635.42%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2093.217 ops/s` | `1875.323 ops/s` | `-10.41%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `2066.725 ops/s` | `2027.196 ops/s` | `-1.91%` | `neutral` |

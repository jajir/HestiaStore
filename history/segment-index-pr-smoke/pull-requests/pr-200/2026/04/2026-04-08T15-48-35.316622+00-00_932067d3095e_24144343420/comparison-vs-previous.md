# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `6482912b40c1a3ba60fd56058c0ca8ed4465cc25`
- Candidate SHA: `932067d3095e951d70ee22cc122c19abacafb3ab`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitAsyncJoin` | `175094.335 ops/s` | `173743.479 ops/s` | `-0.77%` | `neutral` |
| `segment-index-get-live:getHitSync` | `3630538.860 ops/s` | `3504185.736 ops/s` | `-3.48%` | `warning` |
| `segment-index-get-live:getMissAsyncJoin` | `174740.326 ops/s` | `174591.117 ops/s` | `-0.09%` | `neutral` |
| `segment-index-get-live:getMissSync` | `3847359.702 ops/s` | `3690339.558 ops/s` | `-4.08%` | `warning` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `85.620 ops/s` | `75.975 ops/s` | `-11.26%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `88.491 ops/s` | `95.278 ops/s` | `+7.67%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `187042.733 ops/s` | `182786.128 ops/s` | `-2.28%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3909063.957 ops/s` | `3856374.742 ops/s` | `-1.35%` | `neutral` |
| `segment-index-get-persisted:getHitAsyncJoin` | `60483.434 ops/s` | `50857.990 ops/s` | `-15.91%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `107192.553 ops/s` | `102536.823 ops/s` | `-4.34%` | `warning` |
| `segment-index-get-persisted:getMissAsyncJoin` | `177356.855 ops/s` | `176151.316 ops/s` | `-0.68%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `3848009.680 ops/s` | `3946739.598 ops/s` | `+2.57%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2421707.770 ops/s` | `2405229.218 ops/s` | `-0.68%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1458565.301 ops/s` | `1427767.731 ops/s` | `-2.11%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `353150.950 ops/s` | `303445.823 ops/s` | `-14.07%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `188831.009 ops/s` | `163228.508 ops/s` | `-13.56%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `164319.941 ops/s` | `140217.314 ops/s` | `-14.67%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `35686.379 ops/s` | `33922.053 ops/s` | `-4.94%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `31517.081 ops/s` | `27529.619 ops/s` | `-12.65%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `4169.297 ops/s` | `6392.434 ops/s` | `+53.32%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2438.735 ops/s` | `2234.276 ops/s` | `-8.38%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `2505.291 ops/s` | `2629.008 ops/s` | `+4.94%` | `better` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2116.955 ops/s` | `2145.331 ops/s` | `+1.34%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `2313.133 ops/s` | `2485.960 ops/s` | `+7.47%` | `better` |

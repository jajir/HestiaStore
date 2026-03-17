# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `cd0cc92efd820c9eed4f8ca4861418ad2684176a`
- Candidate SHA: `b032f2e4babc8cb3c0692b21f1fe949bf7020cc3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `-` | `59.479 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-cold:getHitSync` | `-` | `67.712 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `-` | `165368.983 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-cold:getMissSync` | `-` | `6570501.277 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `-` | `120.955 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getHitSync` | `-` | `121.985 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `-` | `163599.420 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissSync` | `-` | `5783275.819 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getHitAsyncJoin` | `169860.520 ops/s` | `162968.394 ops/s` | `-4.06%` | `warning` |
| `segment-index-get-overlay:getHitSync` | `5000214.218 ops/s` | `5144553.388 ops/s` | `+2.89%` | `neutral` |
| `segment-index-get-overlay:getMissAsyncJoin` | `-` | `165455.365 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getMissSync` | `-` | `6332279.437 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getHitAsyncJoin` | `59564.880 ops/s` | `57269.725 ops/s` | `-3.85%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `114436.491 ops/s` | `115407.331 ops/s` | `+0.85%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `-` | `164842.955 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getMissSync` | `-` | `7135380.990 ops/s` | `-` | `new` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `555852.245 ops/s` | `495524.289 ops/s` | `-10.85%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `549814.636 ops/s` | `489563.616 ops/s` | `-10.96%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `6037.609 ops/s` | `5960.673 ops/s` | `-1.27%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `270290.826 ops/s` | `270057.167 ops/s` | `-0.09%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `268711.325 ops/s` | `268567.145 ops/s` | `-0.05%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1579.502 ops/s` | `1490.022 ops/s` | `-5.67%` | `warning` |

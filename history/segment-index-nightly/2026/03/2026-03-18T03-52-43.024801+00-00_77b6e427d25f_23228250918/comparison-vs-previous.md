# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `b032f2e4babc8cb3c0692b21f1fe949bf7020cc3`
- Candidate SHA: `77b6e427d25f87af9ee3bd0d0f697c9458a844d1`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `59.479 ops/s` | `70.638 ops/s` | `+18.76%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `67.712 ops/s` | `71.352 ops/s` | `+5.38%` | `better` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `165368.983 ops/s` | `172045.350 ops/s` | `+4.04%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `6570501.277 ops/s` | `6901663.622 ops/s` | `+5.04%` | `better` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `120.955 ops/s` | `115.706 ops/s` | `-4.34%` | `warning` |
| `segment-index-get-multisegment-hot:getHitSync` | `121.985 ops/s` | `116.162 ops/s` | `-4.77%` | `warning` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `163599.420 ops/s` | `172893.118 ops/s` | `+5.68%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `5783275.819 ops/s` | `6608005.535 ops/s` | `+14.26%` | `better` |
| `segment-index-get-overlay:getHitAsyncJoin` | `162968.394 ops/s` | `160145.597 ops/s` | `-1.73%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `5144553.388 ops/s` | `4999500.682 ops/s` | `-2.82%` | `neutral` |
| `segment-index-get-overlay:getMissAsyncJoin` | `165455.365 ops/s` | `169121.308 ops/s` | `+2.22%` | `neutral` |
| `segment-index-get-overlay:getMissSync` | `6332279.437 ops/s` | `6597336.641 ops/s` | `+4.19%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `57269.725 ops/s` | `61316.814 ops/s` | `+7.07%` | `better` |
| `segment-index-get-persisted:getHitSync` | `115407.331 ops/s` | `113793.595 ops/s` | `-1.40%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `164842.955 ops/s` | `169733.642 ops/s` | `+2.97%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `7135380.990 ops/s` | `7252195.701 ops/s` | `+1.64%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `495524.289 ops/s` | `533554.449 ops/s` | `+7.67%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `489563.616 ops/s` | `527571.363 ops/s` | `+7.76%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5960.673 ops/s` | `5983.085 ops/s` | `+0.38%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `270057.167 ops/s` | `273210.844 ops/s` | `+1.17%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `268567.145 ops/s` | `271570.324 ops/s` | `+1.12%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1490.022 ops/s` | `1640.520 ops/s` | `+10.10%` | `better` |

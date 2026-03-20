# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `f110d0a08518e6d6f918649d4aa14d48cfb15719`
- Candidate SHA: `08e7f17bc453491916b66261abc29108631f6159`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `85.090 ops/s` | `101.447 ops/s` | `+19.22%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `85.029 ops/s` | `92.963 ops/s` | `+9.33%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `166494.203 ops/s` | `164149.047 ops/s` | `-1.41%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3725267.945 ops/s` | `3801729.932 ops/s` | `+2.05%` | `neutral` |
| `segment-index-get-overlay:getHitAsyncJoin` | `163100.719 ops/s` | `157133.457 ops/s` | `-3.66%` | `warning` |
| `segment-index-get-overlay:getHitSync` | `4250457.072 ops/s` | `4008980.505 ops/s` | `-5.68%` | `warning` |
| `segment-index-get-overlay:getMissAsyncJoin` | `166881.485 ops/s` | `159665.477 ops/s` | `-4.32%` | `warning` |
| `segment-index-get-overlay:getMissSync` | `3884154.638 ops/s` | `3904674.420 ops/s` | `+0.53%` | `neutral` |
| `segment-index-get-persisted:getHitAsyncJoin` | `56225.545 ops/s` | `55360.203 ops/s` | `-1.54%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `103179.039 ops/s` | `105218.940 ops/s` | `+1.98%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `159276.147 ops/s` | `162543.673 ops/s` | `+2.05%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `4083983.098 ops/s` | `4118720.878 ops/s` | `+0.85%` | `neutral` |
| `segment-index-hot-partition-put:putHotPartition` | `2976455.879 ops/s` | `3062494.813 ops/s` | `+2.89%` | `neutral` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1711631.118 ops/s` | `1712368.988 ops/s` | `+0.04%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `458112.659 ops/s` | `445317.339 ops/s` | `-2.79%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `452902.247 ops/s` | `440224.892 ops/s` | `-2.80%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5210.412 ops/s` | `5092.447 ops/s` | `-2.26%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `206447.676 ops/s` | `197188.603 ops/s` | `-4.48%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `199530.328 ops/s` | `194557.964 ops/s` | `-2.49%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `6917.348 ops/s` | `2630.639 ops/s` | `-61.97%` | `worse` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2351.274 ops/s` | `2352.693 ops/s` | `+0.06%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2674.178 ops/s` | `2638.889 ops/s` | `-1.32%` | `neutral` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2309.773 ops/s` | `2267.984 ops/s` | `-1.81%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `2613.342 ops/s` | `2582.107 ops/s` | `-1.20%` | `neutral` |

# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `18a53ed9711d621c676b011285c6999f34435de1`
- Candidate SHA: `d58b4b8cb93eb11069b7619d68482800be41bb4a`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2037637.807 ops/s` | `2075763.906 ops/s` | `+1.87%` | `neutral` |
| `segment-index-get-live:getMissSync` | `3850796.324 ops/s` | `3765711.029 ops/s` | `-2.21%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `7445.003 ops/s` | `7289.433 ops/s` | `-2.09%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3971923.504 ops/s` | `3981098.786 ops/s` | `+0.23%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `110790.564 ops/s` | `122122.752 ops/s` | `+10.23%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3870912.361 ops/s` | `3759347.543 ops/s` | `-2.88%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2130194.700 ops/s` | `2085567.736 ops/s` | `-2.09%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1066688.198 ops/s` | `1098012.061 ops/s` | `+2.94%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `302729.035 ops/s` | `317370.221 ops/s` | `+4.84%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `147306.164 ops/s` | `136509.569 ops/s` | `-7.33%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `155422.871 ops/s` | `180860.652 ops/s` | `+16.37%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `60289.378 ops/s` | `117635.861 ops/s` | `+95.12%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `47647.131 ops/s` | `110346.870 ops/s` | `+131.59%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `12642.247 ops/s` | `7288.991 ops/s` | `-42.34%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `2661.064 ops/s` | `2802.773 ops/s` | `+5.33%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2759.226 ops/s` | `2602.587 ops/s` | `-5.68%` | `warning` |

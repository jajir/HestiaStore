# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `d60e9c3fc047d39eb0091b6dbadc07b8dc036ce7`
- Candidate SHA: `2f845f9e3bbef0938fce783a33e53a259317ea2a`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2439102.872 ops/s` | `2324691.433 ops/s` | `-4.69%` | `warning` |
| `segment-index-get-live:getMissSync` | `2139756.007 ops/s` | `2233457.551 ops/s` | `+4.38%` | `better` |
| `segment-index-get-persisted:getHitSync` | `1781355.994 ops/s` | `1852670.991 ops/s` | `+4.00%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2127645.203 ops/s` | `2081795.228 ops/s` | `-2.15%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2128234.714 ops/s` | `1837564.030 ops/s` | `-13.66%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1162662.076 ops/s` | `1136870.553 ops/s` | `-2.22%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `295065.153 ops/s` | `313667.790 ops/s` | `+6.30%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `128080.078 ops/s` | `148131.872 ops/s` | `+15.66%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `166985.075 ops/s` | `165535.917 ops/s` | `-0.87%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `181972.404 ops/s` | `174526.500 ops/s` | `-4.09%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `167340.145 ops/s` | `160346.887 ops/s` | `-4.18%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `14632.259 ops/s` | `14179.613 ops/s` | `-3.09%` | `warning` |

# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `d5ffeeaaccc81e4004484e8650d5b9d7fd25e529`
- Candidate SHA: `074505fdb67d1e677828e630cca8323035ffc99e`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2314722.124 ops/s` | `2161540.801 ops/s` | `-6.62%` | `warning` |
| `segment-index-get-live:getMissSync` | `2233599.582 ops/s` | `2219903.057 ops/s` | `-0.61%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `1643409.573 ops/s` | `2057024.915 ops/s` | `+25.17%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2099753.709 ops/s` | `2188192.120 ops/s` | `+4.21%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `1855541.360 ops/s` | `2065908.114 ops/s` | `+11.34%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1096970.566 ops/s` | `1131480.124 ops/s` | `+3.15%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `286596.653 ops/s` | `305084.253 ops/s` | `+6.45%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `116523.939 ops/s` | `157709.043 ops/s` | `+35.34%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `170072.714 ops/s` | `147375.210 ops/s` | `-13.35%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `159224.837 ops/s` | `162336.826 ops/s` | `+1.95%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `145446.104 ops/s` | `147917.716 ops/s` | `+1.70%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `13778.733 ops/s` | `14419.110 ops/s` | `+4.65%` | `better` |

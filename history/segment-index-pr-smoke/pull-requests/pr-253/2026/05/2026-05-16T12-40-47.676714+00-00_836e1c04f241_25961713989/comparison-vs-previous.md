# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `e65add82b78aa708740ea09faa5b47e2f6dcb992`
- Candidate SHA: `836e1c04f2415a6a3deb082239442ff70747b1fe`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `1720434.766 ops/s` | `1881944.120 ops/s` | `+9.39%` | `better` |
| `segment-index-get-live:getMissSync` | `2435065.793 ops/s` | `2652783.497 ops/s` | `+8.94%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `14455.242 ops/s` | `13714.836 ops/s` | `-5.12%` | `warning` |
| `segment-index-get-multisegment-hot:getMissSync` | `2247451.414 ops/s` | `2290179.534 ops/s` | `+1.90%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `1822539.828 ops/s` | `1675125.595 ops/s` | `-8.09%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `2377045.984 ops/s` | `2600774.280 ops/s` | `+9.41%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `1702441.009 ops/s` | `1894951.526 ops/s` | `+11.31%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1013341.609 ops/s` | `1005864.565 ops/s` | `-0.74%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `236511.221 ops/s` | `253376.815 ops/s` | `+7.13%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `84152.361 ops/s` | `115560.451 ops/s` | `+37.32%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `152358.860 ops/s` | `137816.364 ops/s` | `-9.54%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `41935.350 ops/s` | `52262.726 ops/s` | `+24.63%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `36730.443 ops/s` | `46945.946 ops/s` | `+27.81%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `5204.907 ops/s` | `5316.780 ops/s` | `+2.15%` | `neutral` |

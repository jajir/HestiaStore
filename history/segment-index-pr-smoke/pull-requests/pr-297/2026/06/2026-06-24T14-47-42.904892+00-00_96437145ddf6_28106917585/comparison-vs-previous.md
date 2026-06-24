# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `7cc96a4f02588fc1e87970fc84af8f7132a59154`
- Candidate SHA: `96437145ddf64355e7dccbecce330dec158df86a`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2196978.993 ops/s` | `2256990.997 ops/s` | `+2.73%` | `neutral` |
| `segment-index-get-live:getMissSync` | `2069231.992 ops/s` | `2084910.346 ops/s` | `+0.76%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `1692577.125 ops/s` | `1901534.850 ops/s` | `+12.35%` | `better` |
| `segment-index-get-persisted:getMissSync` | `1981478.988 ops/s` | `1984286.070 ops/s` | `+0.14%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2055900.901 ops/s` | `2073149.655 ops/s` | `+0.84%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1111309.966 ops/s` | `1107527.952 ops/s` | `-0.34%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `305090.732 ops/s` | `318657.481 ops/s` | `+4.45%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `153572.158 ops/s` | `165412.222 ops/s` | `+7.71%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `151518.574 ops/s` | `153245.259 ops/s` | `+1.14%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `174234.244 ops/s` | `187821.500 ops/s` | `+7.80%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `161366.163 ops/s` | `172773.862 ops/s` | `+7.07%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `12868.081 ops/s` | `15047.638 ops/s` | `+16.94%` | `better` |

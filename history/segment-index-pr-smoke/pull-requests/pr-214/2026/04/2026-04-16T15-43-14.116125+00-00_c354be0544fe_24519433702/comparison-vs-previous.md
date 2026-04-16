# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `51724b114025e9af1cae85d0e87d4678c8b87310`
- Candidate SHA: `c354be0544fe053e5a292e5a64bab169eca4fb3b`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2754987.116 ops/s` | `2773546.650 ops/s` | `+0.67%` | `neutral` |
| `segment-index-get-live:getMissSync` | `2706654.645 ops/s` | `2797166.941 ops/s` | `+3.34%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `89.403 ops/s` | `83.995 ops/s` | `-6.05%` | `warning` |
| `segment-index-get-multisegment-hot:getMissSync` | `2666125.040 ops/s` | `2567072.328 ops/s` | `-3.72%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `112173.570 ops/s` | `108272.186 ops/s` | `-3.48%` | `warning` |
| `segment-index-get-persisted:getMissSync` | `2477441.537 ops/s` | `2411068.973 ops/s` | `-2.68%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2062618.685 ops/s` | `2302074.843 ops/s` | `+11.61%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1185309.210 ops/s` | `1182259.482 ops/s` | `-0.26%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `329804.268 ops/s` | `323104.931 ops/s` | `-2.03%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `160153.818 ops/s` | `152937.505 ops/s` | `-4.51%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `169650.450 ops/s` | `170167.426 ops/s` | `+0.30%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `7098.511 ops/s` | `35589.854 ops/s` | `+401.37%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `291.608 ops/s` | `2077.137 ops/s` | `+612.30%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `6806.903 ops/s` | `33512.716 ops/s` | `+392.33%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `3670.421 ops/s` | `3769.386 ops/s` | `+2.70%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `3489.633 ops/s` | `3590.400 ops/s` | `+2.89%` | `neutral` |

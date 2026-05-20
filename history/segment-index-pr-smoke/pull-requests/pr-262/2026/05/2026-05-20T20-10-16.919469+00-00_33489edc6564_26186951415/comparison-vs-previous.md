# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `24a53ff220604e4a2d02e63b54909f2f91ff0ce1`
- Candidate SHA: `33489edc656429f5b5dac49ad67d69ed2e30a176`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2340119.446 ops/s` | `2204009.875 ops/s` | `-5.82%` | `warning` |
| `segment-index-get-live:getMissSync` | `2018946.440 ops/s` | `1999952.559 ops/s` | `-0.94%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `1956373.050 ops/s` | `1878677.894 ops/s` | `-3.97%` | `warning` |
| `segment-index-get-persisted:getMissSync` | `2029481.895 ops/s` | `2007145.171 ops/s` | `-1.10%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `1819068.236 ops/s` | `1983984.283 ops/s` | `+9.07%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `995250.647 ops/s` | `1019660.736 ops/s` | `+2.45%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `279327.965 ops/s` | `280246.835 ops/s` | `+0.33%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `116771.019 ops/s` | `126797.782 ops/s` | `+8.59%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `162556.946 ops/s` | `153449.053 ops/s` | `-5.60%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `161132.645 ops/s` | `170392.546 ops/s` | `+5.75%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `147236.924 ops/s` | `156875.028 ops/s` | `+6.55%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `13895.721 ops/s` | `13517.518 ops/s` | `-2.72%` | `neutral` |

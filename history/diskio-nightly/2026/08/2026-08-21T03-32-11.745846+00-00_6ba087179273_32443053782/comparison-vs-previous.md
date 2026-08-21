# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6ba0871792734698e0f788c02cc5eadf23ed5d65`
- Candidate SHA: `6ba0871792734698e0f788c02cc5eadf23ed5d65`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `92.116 ms/op` | `92.000 ms/op` | `-0.13%` | `neutral` |
| `diskio-sequential-read-32k:readSequentialFile` | `63.210 ms/op` | `58.604 ms/op` | `-7.29%` | `worse` |
| `diskio-sequential-read-4k:readSequentialFile` | `74.819 ms/op` | `75.481 ms/op` | `+0.88%` | `neutral` |
| `diskio-sequential-write-1k:writeSequentialFile` | `41.119 ms/op` | `40.315 ms/op` | `-1.96%` | `neutral` |
| `diskio-sequential-write-32k:writeSequentialFile` | `27.039 ms/op` | `26.134 ms/op` | `-3.35%` | `warning` |
| `diskio-sequential-write-4k:writeSequentialFile` | `32.171 ms/op` | `31.581 ms/op` | `-1.83%` | `neutral` |

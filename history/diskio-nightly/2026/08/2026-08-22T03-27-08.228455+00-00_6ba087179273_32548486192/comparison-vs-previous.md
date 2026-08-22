# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6ba0871792734698e0f788c02cc5eadf23ed5d65`
- Candidate SHA: `6ba0871792734698e0f788c02cc5eadf23ed5d65`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `92.000 ms/op` | `82.759 ms/op` | `-10.04%` | `worse` |
| `diskio-sequential-read-32k:readSequentialFile` | `58.604 ms/op` | `62.828 ms/op` | `+7.21%` | `better` |
| `diskio-sequential-read-4k:readSequentialFile` | `75.481 ms/op` | `62.883 ms/op` | `-16.69%` | `worse` |
| `diskio-sequential-write-1k:writeSequentialFile` | `40.315 ms/op` | `39.023 ms/op` | `-3.21%` | `warning` |
| `diskio-sequential-write-32k:writeSequentialFile` | `26.134 ms/op` | `25.603 ms/op` | `-2.03%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `31.581 ms/op` | `30.817 ms/op` | `-2.42%` | `neutral` |

# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `7c9b9709d7b4925f77416d396930aabcca6a6987`
- Candidate SHA: `7c9b9709d7b4925f77416d396930aabcca6a6987`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `90.170 ms/op` | `93.283 ms/op` | `+3.45%` | `better` |
| `diskio-sequential-read-32k:readSequentialFile` | `60.986 ms/op` | `61.666 ms/op` | `+1.12%` | `neutral` |
| `diskio-sequential-read-4k:readSequentialFile` | `66.324 ms/op` | `68.001 ms/op` | `+2.53%` | `neutral` |
| `diskio-sequential-write-1k:writeSequentialFile` | `39.919 ms/op` | `39.872 ms/op` | `-0.12%` | `neutral` |
| `diskio-sequential-write-32k:writeSequentialFile` | `26.155 ms/op` | `26.401 ms/op` | `+0.94%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `31.769 ms/op` | `31.716 ms/op` | `-0.17%` | `neutral` |

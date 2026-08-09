# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `99f6033423f943fa22655f00429ed039230292dc`
- Candidate SHA: `7c9b9709d7b4925f77416d396930aabcca6a6987`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `91.063 ms/op` | `90.170 ms/op` | `-0.98%` | `neutral` |
| `diskio-sequential-read-32k:readSequentialFile` | `58.541 ms/op` | `60.986 ms/op` | `+4.18%` | `better` |
| `diskio-sequential-read-4k:readSequentialFile` | `68.485 ms/op` | `66.324 ms/op` | `-3.16%` | `warning` |
| `diskio-sequential-write-1k:writeSequentialFile` | `40.342 ms/op` | `39.919 ms/op` | `-1.05%` | `neutral` |
| `diskio-sequential-write-32k:writeSequentialFile` | `27.352 ms/op` | `26.155 ms/op` | `-4.38%` | `warning` |
| `diskio-sequential-write-4k:writeSequentialFile` | `31.827 ms/op` | `31.769 ms/op` | `-0.18%` | `neutral` |

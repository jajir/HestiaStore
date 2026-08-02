# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `99f6033423f943fa22655f00429ed039230292dc`
- Candidate SHA: `99f6033423f943fa22655f00429ed039230292dc`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `84.790 ms/op` | `89.624 ms/op` | `+5.70%` | `better` |
| `diskio-sequential-read-32k:readSequentialFile` | `55.111 ms/op` | `53.039 ms/op` | `-3.76%` | `warning` |
| `diskio-sequential-read-4k:readSequentialFile` | `64.155 ms/op` | `62.784 ms/op` | `-2.14%` | `neutral` |
| `diskio-sequential-write-1k:writeSequentialFile` | `39.381 ms/op` | `39.328 ms/op` | `-0.13%` | `neutral` |
| `diskio-sequential-write-32k:writeSequentialFile` | `26.107 ms/op` | `25.858 ms/op` | `-0.95%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `31.413 ms/op` | `30.800 ms/op` | `-1.95%` | `neutral` |

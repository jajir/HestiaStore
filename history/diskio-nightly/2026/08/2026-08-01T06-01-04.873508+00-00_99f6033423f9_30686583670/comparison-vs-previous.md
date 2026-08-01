# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `99f6033423f943fa22655f00429ed039230292dc`
- Candidate SHA: `99f6033423f943fa22655f00429ed039230292dc`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `84.943 ms/op` | `84.790 ms/op` | `-0.18%` | `neutral` |
| `diskio-sequential-read-32k:readSequentialFile` | `55.759 ms/op` | `55.111 ms/op` | `-1.16%` | `neutral` |
| `diskio-sequential-read-4k:readSequentialFile` | `61.233 ms/op` | `64.155 ms/op` | `+4.77%` | `better` |
| `diskio-sequential-write-1k:writeSequentialFile` | `38.300 ms/op` | `39.381 ms/op` | `+2.82%` | `neutral` |
| `diskio-sequential-write-32k:writeSequentialFile` | `25.523 ms/op` | `26.107 ms/op` | `+2.29%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `30.237 ms/op` | `31.413 ms/op` | `+3.89%` | `better` |

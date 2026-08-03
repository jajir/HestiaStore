# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `99f6033423f943fa22655f00429ed039230292dc`
- Candidate SHA: `99f6033423f943fa22655f00429ed039230292dc`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `89.624 ms/op` | `97.405 ms/op` | `+8.68%` | `better` |
| `diskio-sequential-read-32k:readSequentialFile` | `53.039 ms/op` | `58.753 ms/op` | `+10.77%` | `better` |
| `diskio-sequential-read-4k:readSequentialFile` | `62.784 ms/op` | `70.375 ms/op` | `+12.09%` | `better` |
| `diskio-sequential-write-1k:writeSequentialFile` | `39.328 ms/op` | `41.768 ms/op` | `+6.20%` | `better` |
| `diskio-sequential-write-32k:writeSequentialFile` | `25.858 ms/op` | `26.567 ms/op` | `+2.74%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `30.800 ms/op` | `32.137 ms/op` | `+4.34%` | `better` |

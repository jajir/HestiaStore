# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `99f6033423f943fa22655f00429ed039230292dc`
- Candidate SHA: `99f6033423f943fa22655f00429ed039230292dc`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `97.405 ms/op` | `65.147 ms/op` | `-33.12%` | `worse` |
| `diskio-sequential-read-32k:readSequentialFile` | `58.753 ms/op` | `53.596 ms/op` | `-8.78%` | `worse` |
| `diskio-sequential-read-4k:readSequentialFile` | `70.375 ms/op` | `55.330 ms/op` | `-21.38%` | `worse` |
| `diskio-sequential-write-1k:writeSequentialFile` | `41.768 ms/op` | `82.825 ms/op` | `+98.30%` | `better` |
| `diskio-sequential-write-32k:writeSequentialFile` | `26.567 ms/op` | `56.595 ms/op` | `+113.03%` | `better` |
| `diskio-sequential-write-4k:writeSequentialFile` | `32.137 ms/op` | `52.360 ms/op` | `+62.93%` | `better` |

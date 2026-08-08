# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `99f6033423f943fa22655f00429ed039230292dc`
- Candidate SHA: `99f6033423f943fa22655f00429ed039230292dc`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `67.572 ms/op` | `91.063 ms/op` | `+34.77%` | `better` |
| `diskio-sequential-read-32k:readSequentialFile` | `56.129 ms/op` | `58.541 ms/op` | `+4.30%` | `better` |
| `diskio-sequential-read-4k:readSequentialFile` | `59.845 ms/op` | `68.485 ms/op` | `+14.44%` | `better` |
| `diskio-sequential-write-1k:writeSequentialFile` | `77.951 ms/op` | `40.342 ms/op` | `-48.25%` | `worse` |
| `diskio-sequential-write-32k:writeSequentialFile` | `96.157 ms/op` | `27.352 ms/op` | `-71.55%` | `worse` |
| `diskio-sequential-write-4k:writeSequentialFile` | `85.247 ms/op` | `31.827 ms/op` | `-62.66%` | `worse` |

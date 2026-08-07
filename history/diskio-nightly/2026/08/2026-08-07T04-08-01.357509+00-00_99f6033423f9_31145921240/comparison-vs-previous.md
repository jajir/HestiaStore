# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `99f6033423f943fa22655f00429ed039230292dc`
- Candidate SHA: `99f6033423f943fa22655f00429ed039230292dc`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `82.866 ms/op` | `67.572 ms/op` | `-18.46%` | `worse` |
| `diskio-sequential-read-32k:readSequentialFile` | `54.343 ms/op` | `56.129 ms/op` | `+3.29%` | `better` |
| `diskio-sequential-read-4k:readSequentialFile` | `60.584 ms/op` | `59.845 ms/op` | `-1.22%` | `neutral` |
| `diskio-sequential-write-1k:writeSequentialFile` | `37.809 ms/op` | `77.951 ms/op` | `+106.17%` | `better` |
| `diskio-sequential-write-32k:writeSequentialFile` | `25.493 ms/op` | `96.157 ms/op` | `+277.19%` | `better` |
| `diskio-sequential-write-4k:writeSequentialFile` | `30.217 ms/op` | `85.247 ms/op` | `+182.11%` | `better` |

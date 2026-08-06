# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `99f6033423f943fa22655f00429ed039230292dc`
- Candidate SHA: `99f6033423f943fa22655f00429ed039230292dc`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `83.516 ms/op` | `82.866 ms/op` | `-0.78%` | `neutral` |
| `diskio-sequential-read-32k:readSequentialFile` | `65.147 ms/op` | `54.343 ms/op` | `-16.58%` | `worse` |
| `diskio-sequential-read-4k:readSequentialFile` | `61.050 ms/op` | `60.584 ms/op` | `-0.76%` | `neutral` |
| `diskio-sequential-write-1k:writeSequentialFile` | `39.315 ms/op` | `37.809 ms/op` | `-3.83%` | `warning` |
| `diskio-sequential-write-32k:writeSequentialFile` | `26.158 ms/op` | `25.493 ms/op` | `-2.54%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `31.094 ms/op` | `30.217 ms/op` | `-2.82%` | `neutral` |

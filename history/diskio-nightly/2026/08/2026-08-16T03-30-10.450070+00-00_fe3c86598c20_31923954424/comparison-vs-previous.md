# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `fe3c86598c2019fd5ff45fef2635519ddcc4ebb3`
- Candidate SHA: `fe3c86598c2019fd5ff45fef2635519ddcc4ebb3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `91.512 ms/op` | `61.348 ms/op` | `-32.96%` | `worse` |
| `diskio-sequential-read-32k:readSequentialFile` | `58.343 ms/op` | `51.428 ms/op` | `-11.85%` | `worse` |
| `diskio-sequential-read-4k:readSequentialFile` | `68.277 ms/op` | `53.316 ms/op` | `-21.91%` | `worse` |
| `diskio-sequential-write-1k:writeSequentialFile` | `40.135 ms/op` | `55.274 ms/op` | `+37.72%` | `better` |
| `diskio-sequential-write-32k:writeSequentialFile` | `26.199 ms/op` | `47.178 ms/op` | `+80.08%` | `better` |
| `diskio-sequential-write-4k:writeSequentialFile` | `31.681 ms/op` | `47.155 ms/op` | `+48.84%` | `better` |

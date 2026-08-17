# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `fe3c86598c2019fd5ff45fef2635519ddcc4ebb3`
- Candidate SHA: `fe3c86598c2019fd5ff45fef2635519ddcc4ebb3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `61.348 ms/op` | `84.550 ms/op` | `+37.82%` | `better` |
| `diskio-sequential-read-32k:readSequentialFile` | `51.428 ms/op` | `56.190 ms/op` | `+9.26%` | `better` |
| `diskio-sequential-read-4k:readSequentialFile` | `53.316 ms/op` | `62.600 ms/op` | `+17.41%` | `better` |
| `diskio-sequential-write-1k:writeSequentialFile` | `55.274 ms/op` | `38.018 ms/op` | `-31.22%` | `worse` |
| `diskio-sequential-write-32k:writeSequentialFile` | `47.178 ms/op` | `25.570 ms/op` | `-45.80%` | `worse` |
| `diskio-sequential-write-4k:writeSequentialFile` | `47.155 ms/op` | `30.463 ms/op` | `-35.40%` | `worse` |

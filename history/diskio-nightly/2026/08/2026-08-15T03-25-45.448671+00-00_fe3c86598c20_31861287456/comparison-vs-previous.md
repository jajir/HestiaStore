# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `fe3c86598c2019fd5ff45fef2635519ddcc4ebb3`
- Candidate SHA: `fe3c86598c2019fd5ff45fef2635519ddcc4ebb3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `82.823 ms/op` | `91.512 ms/op` | `+10.49%` | `better` |
| `diskio-sequential-read-32k:readSequentialFile` | `56.237 ms/op` | `58.343 ms/op` | `+3.74%` | `better` |
| `diskio-sequential-read-4k:readSequentialFile` | `61.158 ms/op` | `68.277 ms/op` | `+11.64%` | `better` |
| `diskio-sequential-write-1k:writeSequentialFile` | `38.766 ms/op` | `40.135 ms/op` | `+3.53%` | `better` |
| `diskio-sequential-write-32k:writeSequentialFile` | `25.605 ms/op` | `26.199 ms/op` | `+2.32%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `30.361 ms/op` | `31.681 ms/op` | `+4.35%` | `better` |

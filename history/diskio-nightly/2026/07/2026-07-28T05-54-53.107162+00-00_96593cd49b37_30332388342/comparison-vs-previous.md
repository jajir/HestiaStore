# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `96593cd49b374380016362f1fc4d5bacdf709832`
- Candidate SHA: `96593cd49b374380016362f1fc4d5bacdf709832`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `83.752 ms/op` | `90.908 ms/op` | `+8.54%` | `better` |
| `diskio-sequential-read-32k:readSequentialFile` | `56.632 ms/op` | `54.577 ms/op` | `-3.63%` | `warning` |
| `diskio-sequential-read-4k:readSequentialFile` | `62.146 ms/op` | `65.251 ms/op` | `+5.00%` | `better` |
| `diskio-sequential-write-1k:writeSequentialFile` | `38.293 ms/op` | `39.124 ms/op` | `+2.17%` | `neutral` |
| `diskio-sequential-write-32k:writeSequentialFile` | `25.735 ms/op` | `25.833 ms/op` | `+0.38%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `30.236 ms/op` | `31.082 ms/op` | `+2.80%` | `neutral` |

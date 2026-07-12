# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `8b88b599997497df9b956d4be577a2b7424cedcb`
- Candidate SHA: `8b88b599997497df9b956d4be577a2b7424cedcb`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `83.685 ms/op` | `89.086 ms/op` | `+6.45%` | `better` |
| `diskio-sequential-read-32k:readSequentialFile` | `55.068 ms/op` | `57.054 ms/op` | `+3.61%` | `better` |
| `diskio-sequential-read-4k:readSequentialFile` | `61.810 ms/op` | `61.355 ms/op` | `-0.74%` | `neutral` |
| `diskio-sequential-write-1k:writeSequentialFile` | `39.671 ms/op` | `39.359 ms/op` | `-0.79%` | `neutral` |
| `diskio-sequential-write-32k:writeSequentialFile` | `25.757 ms/op` | `25.772 ms/op` | `+0.06%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `31.026 ms/op` | `31.708 ms/op` | `+2.20%` | `neutral` |

# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `8b88b599997497df9b956d4be577a2b7424cedcb`
- Candidate SHA: `8b88b599997497df9b956d4be577a2b7424cedcb`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `96.365 ms/op` | `83.685 ms/op` | `-13.16%` | `worse` |
| `diskio-sequential-read-32k:readSequentialFile` | `59.983 ms/op` | `55.068 ms/op` | `-8.19%` | `worse` |
| `diskio-sequential-read-4k:readSequentialFile` | `68.373 ms/op` | `61.810 ms/op` | `-9.60%` | `worse` |
| `diskio-sequential-write-1k:writeSequentialFile` | `41.496 ms/op` | `39.671 ms/op` | `-4.40%` | `warning` |
| `diskio-sequential-write-32k:writeSequentialFile` | `26.972 ms/op` | `25.757 ms/op` | `-4.50%` | `warning` |
| `diskio-sequential-write-4k:writeSequentialFile` | `32.424 ms/op` | `31.026 ms/op` | `-4.31%` | `warning` |

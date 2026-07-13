# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `8b88b599997497df9b956d4be577a2b7424cedcb`
- Candidate SHA: `8b88b599997497df9b956d4be577a2b7424cedcb`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `89.086 ms/op` | `83.482 ms/op` | `-6.29%` | `warning` |
| `diskio-sequential-read-32k:readSequentialFile` | `57.054 ms/op` | `55.357 ms/op` | `-2.97%` | `neutral` |
| `diskio-sequential-read-4k:readSequentialFile` | `61.355 ms/op` | `62.683 ms/op` | `+2.16%` | `neutral` |
| `diskio-sequential-write-1k:writeSequentialFile` | `39.359 ms/op` | `39.326 ms/op` | `-0.08%` | `neutral` |
| `diskio-sequential-write-32k:writeSequentialFile` | `25.772 ms/op` | `26.148 ms/op` | `+1.46%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `31.708 ms/op` | `31.347 ms/op` | `-1.14%` | `neutral` |

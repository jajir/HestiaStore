# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `84.355 ms/op` | `83.967 ms/op` | `-0.46%` | `neutral` |
| `diskio-sequential-read-32k:readSequentialFile` | `64.371 ms/op` | `56.993 ms/op` | `-11.46%` | `worse` |
| `diskio-sequential-read-4k:readSequentialFile` | `61.268 ms/op` | `62.480 ms/op` | `+1.98%` | `neutral` |
| `diskio-sequential-write-1k:writeSequentialFile` | `38.722 ms/op` | `39.300 ms/op` | `+1.49%` | `neutral` |
| `diskio-sequential-write-32k:writeSequentialFile` | `26.633 ms/op` | `26.519 ms/op` | `-0.43%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `31.335 ms/op` | `31.042 ms/op` | `-0.94%` | `neutral` |

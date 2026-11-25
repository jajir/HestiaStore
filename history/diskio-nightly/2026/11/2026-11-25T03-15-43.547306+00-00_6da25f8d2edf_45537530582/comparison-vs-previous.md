# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `89.114 ms/op` | `81.180 ms/op` | `-8.90%` | `worse` |
| `diskio-sequential-read-32k:readSequentialFile` | `58.951 ms/op` | `59.683 ms/op` | `+1.24%` | `neutral` |
| `diskio-sequential-read-4k:readSequentialFile` | `63.458 ms/op` | `61.627 ms/op` | `-2.88%` | `neutral` |
| `diskio-sequential-write-1k:writeSequentialFile` | `41.264 ms/op` | `38.908 ms/op` | `-5.71%` | `warning` |
| `diskio-sequential-write-32k:writeSequentialFile` | `29.210 ms/op` | `26.860 ms/op` | `-8.05%` | `worse` |
| `diskio-sequential-write-4k:writeSequentialFile` | `32.168 ms/op` | `31.776 ms/op` | `-1.22%` | `neutral` |

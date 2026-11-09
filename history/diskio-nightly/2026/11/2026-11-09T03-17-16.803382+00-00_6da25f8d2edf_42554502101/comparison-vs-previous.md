# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `80.617 ms/op` | `75.611 ms/op` | `-6.21%` | `warning` |
| `diskio-sequential-read-32k:readSequentialFile` | `69.467 ms/op` | `50.570 ms/op` | `-27.20%` | `worse` |
| `diskio-sequential-read-4k:readSequentialFile` | `75.722 ms/op` | `55.407 ms/op` | `-26.83%` | `worse` |
| `diskio-sequential-write-1k:writeSequentialFile` | `41.190 ms/op` | `39.693 ms/op` | `-3.64%` | `warning` |
| `diskio-sequential-write-32k:writeSequentialFile` | `37.470 ms/op` | `36.800 ms/op` | `-1.79%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `37.021 ms/op` | `35.152 ms/op` | `-5.05%` | `warning` |

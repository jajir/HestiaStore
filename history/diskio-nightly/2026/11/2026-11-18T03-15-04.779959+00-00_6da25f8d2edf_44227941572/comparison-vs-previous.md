# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `68.224 ms/op` | `68.309 ms/op` | `+0.12%` | `neutral` |
| `diskio-sequential-read-32k:readSequentialFile` | `57.548 ms/op` | `56.064 ms/op` | `-2.58%` | `neutral` |
| `diskio-sequential-read-4k:readSequentialFile` | `58.703 ms/op` | `59.825 ms/op` | `+1.91%` | `neutral` |
| `diskio-sequential-write-1k:writeSequentialFile` | `30.732 ms/op` | `31.111 ms/op` | `+1.23%` | `neutral` |
| `diskio-sequential-write-32k:writeSequentialFile` | `45.073 ms/op` | `38.863 ms/op` | `-13.78%` | `worse` |
| `diskio-sequential-write-4k:writeSequentialFile` | `34.498 ms/op` | `34.407 ms/op` | `-0.26%` | `neutral` |

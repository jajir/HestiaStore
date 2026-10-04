# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `72.238 ms/op` | `64.279 ms/op` | `-11.02%` | `worse` |
| `diskio-sequential-read-32k:readSequentialFile` | `48.044 ms/op` | `48.730 ms/op` | `+1.43%` | `neutral` |
| `diskio-sequential-read-4k:readSequentialFile` | `54.770 ms/op` | `51.347 ms/op` | `-6.25%` | `warning` |
| `diskio-sequential-write-1k:writeSequentialFile` | `35.700 ms/op` | `35.425 ms/op` | `-0.77%` | `neutral` |
| `diskio-sequential-write-32k:writeSequentialFile` | `46.107 ms/op` | `32.882 ms/op` | `-28.68%` | `worse` |
| `diskio-sequential-write-4k:writeSequentialFile` | `34.882 ms/op` | `37.316 ms/op` | `+6.98%` | `better` |

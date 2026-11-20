# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `83.261 ms/op` | `93.766 ms/op` | `+12.62%` | `better` |
| `diskio-sequential-read-32k:readSequentialFile` | `55.821 ms/op` | `61.875 ms/op` | `+10.85%` | `better` |
| `diskio-sequential-read-4k:readSequentialFile` | `62.348 ms/op` | `69.825 ms/op` | `+11.99%` | `better` |
| `diskio-sequential-write-1k:writeSequentialFile` | `38.735 ms/op` | `41.654 ms/op` | `+7.54%` | `better` |
| `diskio-sequential-write-32k:writeSequentialFile` | `26.804 ms/op` | `26.511 ms/op` | `-1.09%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `32.046 ms/op` | `32.279 ms/op` | `+0.73%` | `neutral` |

# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `75.972 ms/op` | `80.617 ms/op` | `+6.11%` | `better` |
| `diskio-sequential-read-32k:readSequentialFile` | `48.554 ms/op` | `69.467 ms/op` | `+43.07%` | `better` |
| `diskio-sequential-read-4k:readSequentialFile` | `55.147 ms/op` | `75.722 ms/op` | `+37.31%` | `better` |
| `diskio-sequential-write-1k:writeSequentialFile` | `35.800 ms/op` | `41.190 ms/op` | `+15.06%` | `better` |
| `diskio-sequential-write-32k:writeSequentialFile` | `33.688 ms/op` | `37.470 ms/op` | `+11.23%` | `better` |
| `diskio-sequential-write-4k:writeSequentialFile` | `46.354 ms/op` | `37.021 ms/op` | `-20.13%` | `worse` |

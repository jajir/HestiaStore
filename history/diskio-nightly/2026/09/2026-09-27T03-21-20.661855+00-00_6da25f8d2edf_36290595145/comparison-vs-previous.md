# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `92.218 ms/op` | `69.645 ms/op` | `-24.48%` | `worse` |
| `diskio-sequential-read-32k:readSequentialFile` | `67.626 ms/op` | `51.259 ms/op` | `-24.20%` | `worse` |
| `diskio-sequential-read-4k:readSequentialFile` | `67.826 ms/op` | `59.076 ms/op` | `-12.90%` | `worse` |
| `diskio-sequential-write-1k:writeSequentialFile` | `39.776 ms/op` | `50.675 ms/op` | `+27.40%` | `better` |
| `diskio-sequential-write-32k:writeSequentialFile` | `26.533 ms/op` | `62.786 ms/op` | `+136.63%` | `better` |
| `diskio-sequential-write-4k:writeSequentialFile` | `31.245 ms/op` | `70.505 ms/op` | `+125.65%` | `better` |

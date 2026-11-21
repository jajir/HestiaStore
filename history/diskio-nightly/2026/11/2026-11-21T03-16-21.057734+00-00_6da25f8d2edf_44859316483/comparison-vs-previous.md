# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `93.766 ms/op` | `88.799 ms/op` | `-5.30%` | `warning` |
| `diskio-sequential-read-32k:readSequentialFile` | `61.875 ms/op` | `76.657 ms/op` | `+23.89%` | `better` |
| `diskio-sequential-read-4k:readSequentialFile` | `69.825 ms/op` | `79.595 ms/op` | `+13.99%` | `better` |
| `diskio-sequential-write-1k:writeSequentialFile` | `41.654 ms/op` | `57.933 ms/op` | `+39.08%` | `better` |
| `diskio-sequential-write-32k:writeSequentialFile` | `26.511 ms/op` | `41.076 ms/op` | `+54.94%` | `better` |
| `diskio-sequential-write-4k:writeSequentialFile` | `32.279 ms/op` | `42.946 ms/op` | `+33.05%` | `better` |

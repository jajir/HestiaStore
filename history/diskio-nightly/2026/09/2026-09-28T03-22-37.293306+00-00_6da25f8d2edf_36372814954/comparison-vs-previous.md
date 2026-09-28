# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `69.645 ms/op` | `80.003 ms/op` | `+14.87%` | `better` |
| `diskio-sequential-read-32k:readSequentialFile` | `51.259 ms/op` | `56.059 ms/op` | `+9.37%` | `better` |
| `diskio-sequential-read-4k:readSequentialFile` | `59.076 ms/op` | `62.513 ms/op` | `+5.82%` | `better` |
| `diskio-sequential-write-1k:writeSequentialFile` | `50.675 ms/op` | `38.429 ms/op` | `-24.17%` | `worse` |
| `diskio-sequential-write-32k:writeSequentialFile` | `62.786 ms/op` | `26.289 ms/op` | `-58.13%` | `worse` |
| `diskio-sequential-write-4k:writeSequentialFile` | `70.505 ms/op` | `30.516 ms/op` | `-56.72%` | `worse` |

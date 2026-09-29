# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `80.003 ms/op` | `72.773 ms/op` | `-9.04%` | `worse` |
| `diskio-sequential-read-32k:readSequentialFile` | `56.059 ms/op` | `47.930 ms/op` | `-14.50%` | `worse` |
| `diskio-sequential-read-4k:readSequentialFile` | `62.513 ms/op` | `56.319 ms/op` | `-9.91%` | `worse` |
| `diskio-sequential-write-1k:writeSequentialFile` | `38.429 ms/op` | `36.556 ms/op` | `-4.87%` | `warning` |
| `diskio-sequential-write-32k:writeSequentialFile` | `26.289 ms/op` | `32.977 ms/op` | `+25.44%` | `better` |
| `diskio-sequential-write-4k:writeSequentialFile` | `30.516 ms/op` | `31.693 ms/op` | `+3.86%` | `better` |

# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `67.577 ms/op` | `63.625 ms/op` | `-5.85%` | `warning` |
| `diskio-sequential-read-32k:readSequentialFile` | `54.823 ms/op` | `47.140 ms/op` | `-14.01%` | `worse` |
| `diskio-sequential-read-4k:readSequentialFile` | `58.176 ms/op` | `49.133 ms/op` | `-15.54%` | `worse` |
| `diskio-sequential-write-1k:writeSequentialFile` | `30.495 ms/op` | `42.195 ms/op` | `+38.37%` | `better` |
| `diskio-sequential-write-32k:writeSequentialFile` | `38.982 ms/op` | `31.973 ms/op` | `-17.98%` | `worse` |
| `diskio-sequential-write-4k:writeSequentialFile` | `34.501 ms/op` | `30.705 ms/op` | `-11.00%` | `worse` |

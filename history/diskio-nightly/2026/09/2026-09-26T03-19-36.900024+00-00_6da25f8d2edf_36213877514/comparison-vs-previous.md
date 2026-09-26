# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `82.289 ms/op` | `92.218 ms/op` | `+12.07%` | `better` |
| `diskio-sequential-read-32k:readSequentialFile` | `56.382 ms/op` | `67.626 ms/op` | `+19.94%` | `better` |
| `diskio-sequential-read-4k:readSequentialFile` | `60.300 ms/op` | `67.826 ms/op` | `+12.48%` | `better` |
| `diskio-sequential-write-1k:writeSequentialFile` | `39.585 ms/op` | `39.776 ms/op` | `+0.48%` | `neutral` |
| `diskio-sequential-write-32k:writeSequentialFile` | `26.445 ms/op` | `26.533 ms/op` | `+0.33%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `31.678 ms/op` | `31.245 ms/op` | `-1.37%` | `neutral` |

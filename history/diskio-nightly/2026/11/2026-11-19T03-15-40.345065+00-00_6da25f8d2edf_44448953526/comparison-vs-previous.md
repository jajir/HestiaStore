# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `68.309 ms/op` | `83.261 ms/op` | `+21.89%` | `better` |
| `diskio-sequential-read-32k:readSequentialFile` | `56.064 ms/op` | `55.821 ms/op` | `-0.43%` | `neutral` |
| `diskio-sequential-read-4k:readSequentialFile` | `59.825 ms/op` | `62.348 ms/op` | `+4.22%` | `better` |
| `diskio-sequential-write-1k:writeSequentialFile` | `31.111 ms/op` | `38.735 ms/op` | `+24.50%` | `better` |
| `diskio-sequential-write-32k:writeSequentialFile` | `38.863 ms/op` | `26.804 ms/op` | `-31.03%` | `worse` |
| `diskio-sequential-write-4k:writeSequentialFile` | `34.407 ms/op` | `32.046 ms/op` | `-6.86%` | `warning` |

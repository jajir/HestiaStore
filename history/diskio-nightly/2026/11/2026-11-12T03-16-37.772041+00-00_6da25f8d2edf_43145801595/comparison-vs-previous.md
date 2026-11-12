# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `83.967 ms/op` | `73.182 ms/op` | `-12.84%` | `worse` |
| `diskio-sequential-read-32k:readSequentialFile` | `56.993 ms/op` | `50.308 ms/op` | `-11.73%` | `worse` |
| `diskio-sequential-read-4k:readSequentialFile` | `62.480 ms/op` | `56.788 ms/op` | `-9.11%` | `worse` |
| `diskio-sequential-write-1k:writeSequentialFile` | `39.300 ms/op` | `36.195 ms/op` | `-7.90%` | `worse` |
| `diskio-sequential-write-32k:writeSequentialFile` | `26.519 ms/op` | `35.684 ms/op` | `+34.56%` | `better` |
| `diskio-sequential-write-4k:writeSequentialFile` | `31.042 ms/op` | `45.667 ms/op` | `+47.11%` | `better` |

# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `71.832 ms/op` | `83.178 ms/op` | `+15.80%` | `better` |
| `diskio-sequential-read-32k:readSequentialFile` | `46.254 ms/op` | `57.957 ms/op` | `+25.30%` | `better` |
| `diskio-sequential-read-4k:readSequentialFile` | `52.678 ms/op` | `68.485 ms/op` | `+30.01%` | `better` |
| `diskio-sequential-write-1k:writeSequentialFile` | `36.893 ms/op` | `38.670 ms/op` | `+4.82%` | `better` |
| `diskio-sequential-write-32k:writeSequentialFile` | `33.874 ms/op` | `26.920 ms/op` | `-20.53%` | `worse` |
| `diskio-sequential-write-4k:writeSequentialFile` | `34.327 ms/op` | `31.701 ms/op` | `-7.65%` | `worse` |

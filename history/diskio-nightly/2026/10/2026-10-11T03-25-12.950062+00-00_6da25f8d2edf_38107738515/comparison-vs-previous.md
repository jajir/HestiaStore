# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `80.960 ms/op` | `73.739 ms/op` | `-8.92%` | `worse` |
| `diskio-sequential-read-32k:readSequentialFile` | `56.660 ms/op` | `50.535 ms/op` | `-10.81%` | `worse` |
| `diskio-sequential-read-4k:readSequentialFile` | `60.072 ms/op` | `53.553 ms/op` | `-10.85%` | `worse` |
| `diskio-sequential-write-1k:writeSequentialFile` | `38.278 ms/op` | `41.998 ms/op` | `+9.72%` | `better` |
| `diskio-sequential-write-32k:writeSequentialFile` | `26.245 ms/op` | `32.421 ms/op` | `+23.53%` | `better` |
| `diskio-sequential-write-4k:writeSequentialFile` | `30.427 ms/op` | `34.313 ms/op` | `+12.77%` | `better` |

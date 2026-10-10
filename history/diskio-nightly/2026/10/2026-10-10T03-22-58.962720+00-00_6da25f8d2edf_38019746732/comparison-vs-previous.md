# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `75.690 ms/op` | `80.960 ms/op` | `+6.96%` | `better` |
| `diskio-sequential-read-32k:readSequentialFile` | `60.811 ms/op` | `56.660 ms/op` | `-6.83%` | `warning` |
| `diskio-sequential-read-4k:readSequentialFile` | `63.717 ms/op` | `60.072 ms/op` | `-5.72%` | `warning` |
| `diskio-sequential-write-1k:writeSequentialFile` | `36.386 ms/op` | `38.278 ms/op` | `+5.20%` | `better` |
| `diskio-sequential-write-32k:writeSequentialFile` | `31.401 ms/op` | `26.245 ms/op` | `-16.42%` | `worse` |
| `diskio-sequential-write-4k:writeSequentialFile` | `41.675 ms/op` | `30.427 ms/op` | `-26.99%` | `worse` |

# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `82.589 ms/op` | `75.690 ms/op` | `-8.35%` | `worse` |
| `diskio-sequential-read-32k:readSequentialFile` | `56.395 ms/op` | `60.811 ms/op` | `+7.83%` | `better` |
| `diskio-sequential-read-4k:readSequentialFile` | `67.889 ms/op` | `63.717 ms/op` | `-6.15%` | `warning` |
| `diskio-sequential-write-1k:writeSequentialFile` | `38.885 ms/op` | `36.386 ms/op` | `-6.43%` | `warning` |
| `diskio-sequential-write-32k:writeSequentialFile` | `26.583 ms/op` | `31.401 ms/op` | `+18.12%` | `better` |
| `diskio-sequential-write-4k:writeSequentialFile` | `31.771 ms/op` | `41.675 ms/op` | `+31.17%` | `better` |

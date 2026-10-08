# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `63.759 ms/op` | `82.589 ms/op` | `+29.53%` | `better` |
| `diskio-sequential-read-32k:readSequentialFile` | `47.430 ms/op` | `56.395 ms/op` | `+18.90%` | `better` |
| `diskio-sequential-read-4k:readSequentialFile` | `50.001 ms/op` | `67.889 ms/op` | `+35.78%` | `better` |
| `diskio-sequential-write-1k:writeSequentialFile` | `34.673 ms/op` | `38.885 ms/op` | `+12.15%` | `better` |
| `diskio-sequential-write-32k:writeSequentialFile` | `33.202 ms/op` | `26.583 ms/op` | `-19.94%` | `worse` |
| `diskio-sequential-write-4k:writeSequentialFile` | `33.040 ms/op` | `31.771 ms/op` | `-3.84%` | `warning` |

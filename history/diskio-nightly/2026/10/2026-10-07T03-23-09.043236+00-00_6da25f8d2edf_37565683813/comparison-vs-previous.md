# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `81.281 ms/op` | `63.759 ms/op` | `-21.56%` | `worse` |
| `diskio-sequential-read-32k:readSequentialFile` | `53.124 ms/op` | `47.430 ms/op` | `-10.72%` | `worse` |
| `diskio-sequential-read-4k:readSequentialFile` | `61.551 ms/op` | `50.001 ms/op` | `-18.77%` | `worse` |
| `diskio-sequential-write-1k:writeSequentialFile` | `38.716 ms/op` | `34.673 ms/op` | `-10.44%` | `worse` |
| `diskio-sequential-write-32k:writeSequentialFile` | `26.657 ms/op` | `33.202 ms/op` | `+24.55%` | `better` |
| `diskio-sequential-write-4k:writeSequentialFile` | `30.812 ms/op` | `33.040 ms/op` | `+7.23%` | `better` |

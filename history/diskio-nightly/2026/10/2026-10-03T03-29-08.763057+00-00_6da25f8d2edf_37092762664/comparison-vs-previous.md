# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `82.002 ms/op` | `72.238 ms/op` | `-11.91%` | `worse` |
| `diskio-sequential-read-32k:readSequentialFile` | `64.077 ms/op` | `48.044 ms/op` | `-25.02%` | `worse` |
| `diskio-sequential-read-4k:readSequentialFile` | `67.841 ms/op` | `54.770 ms/op` | `-19.27%` | `worse` |
| `diskio-sequential-write-1k:writeSequentialFile` | `38.040 ms/op` | `35.700 ms/op` | `-6.15%` | `warning` |
| `diskio-sequential-write-32k:writeSequentialFile` | `26.136 ms/op` | `46.107 ms/op` | `+76.41%` | `better` |
| `diskio-sequential-write-4k:writeSequentialFile` | `30.417 ms/op` | `34.882 ms/op` | `+14.68%` | `better` |

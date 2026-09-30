# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `72.773 ms/op` | `83.931 ms/op` | `+15.33%` | `better` |
| `diskio-sequential-read-32k:readSequentialFile` | `47.930 ms/op` | `58.702 ms/op` | `+22.47%` | `better` |
| `diskio-sequential-read-4k:readSequentialFile` | `56.319 ms/op` | `62.259 ms/op` | `+10.55%` | `better` |
| `diskio-sequential-write-1k:writeSequentialFile` | `36.556 ms/op` | `38.798 ms/op` | `+6.13%` | `better` |
| `diskio-sequential-write-32k:writeSequentialFile` | `32.977 ms/op` | `26.888 ms/op` | `-18.47%` | `worse` |
| `diskio-sequential-write-4k:writeSequentialFile` | `31.693 ms/op` | `31.292 ms/op` | `-1.27%` | `neutral` |

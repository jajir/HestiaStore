# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `84.138 ms/op` | `80.792 ms/op` | `-3.98%` | `warning` |
| `diskio-sequential-read-32k:readSequentialFile` | `56.949 ms/op` | `55.112 ms/op` | `-3.23%` | `warning` |
| `diskio-sequential-read-4k:readSequentialFile` | `62.601 ms/op` | `60.903 ms/op` | `-2.71%` | `neutral` |
| `diskio-sequential-write-1k:writeSequentialFile` | `39.759 ms/op` | `40.376 ms/op` | `+1.55%` | `neutral` |
| `diskio-sequential-write-32k:writeSequentialFile` | `26.989 ms/op` | `26.399 ms/op` | `-2.19%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `31.406 ms/op` | `30.825 ms/op` | `-1.85%` | `neutral` |

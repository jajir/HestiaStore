# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `80.792 ms/op` | `85.386 ms/op` | `+5.69%` | `better` |
| `diskio-sequential-read-32k:readSequentialFile` | `55.112 ms/op` | `56.113 ms/op` | `+1.82%` | `neutral` |
| `diskio-sequential-read-4k:readSequentialFile` | `60.903 ms/op` | `60.988 ms/op` | `+0.14%` | `neutral` |
| `diskio-sequential-write-1k:writeSequentialFile` | `40.376 ms/op` | `38.332 ms/op` | `-5.06%` | `warning` |
| `diskio-sequential-write-32k:writeSequentialFile` | `26.399 ms/op` | `26.521 ms/op` | `+0.46%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `30.825 ms/op` | `31.438 ms/op` | `+1.99%` | `neutral` |

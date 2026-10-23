# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `81.342 ms/op` | `83.482 ms/op` | `+2.63%` | `neutral` |
| `diskio-sequential-read-32k:readSequentialFile` | `71.292 ms/op` | `56.223 ms/op` | `-21.14%` | `worse` |
| `diskio-sequential-read-4k:readSequentialFile` | `75.815 ms/op` | `62.698 ms/op` | `-17.30%` | `worse` |
| `diskio-sequential-write-1k:writeSequentialFile` | `40.348 ms/op` | `39.645 ms/op` | `-1.74%` | `neutral` |
| `diskio-sequential-write-32k:writeSequentialFile` | `35.696 ms/op` | `26.693 ms/op` | `-25.22%` | `worse` |
| `diskio-sequential-write-4k:writeSequentialFile` | `36.945 ms/op` | `31.218 ms/op` | `-15.50%` | `worse` |

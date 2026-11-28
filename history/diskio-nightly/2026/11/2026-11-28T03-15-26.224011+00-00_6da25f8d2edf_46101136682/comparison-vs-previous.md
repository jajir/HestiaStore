# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `81.114 ms/op` | `81.855 ms/op` | `+0.91%` | `neutral` |
| `diskio-sequential-read-32k:readSequentialFile` | `55.067 ms/op` | `54.796 ms/op` | `-0.49%` | `neutral` |
| `diskio-sequential-read-4k:readSequentialFile` | `61.295 ms/op` | `62.908 ms/op` | `+2.63%` | `neutral` |
| `diskio-sequential-write-1k:writeSequentialFile` | `40.255 ms/op` | `40.865 ms/op` | `+1.52%` | `neutral` |
| `diskio-sequential-write-32k:writeSequentialFile` | `27.552 ms/op` | `26.812 ms/op` | `-2.68%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `31.838 ms/op` | `31.420 ms/op` | `-1.31%` | `neutral` |

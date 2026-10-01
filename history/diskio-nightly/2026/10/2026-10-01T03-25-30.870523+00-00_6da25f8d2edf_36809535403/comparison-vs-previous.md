# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `83.931 ms/op` | `93.807 ms/op` | `+11.77%` | `better` |
| `diskio-sequential-read-32k:readSequentialFile` | `58.702 ms/op` | `68.529 ms/op` | `+16.74%` | `better` |
| `diskio-sequential-read-4k:readSequentialFile` | `62.259 ms/op` | `67.696 ms/op` | `+8.73%` | `better` |
| `diskio-sequential-write-1k:writeSequentialFile` | `38.798 ms/op` | `40.542 ms/op` | `+4.49%` | `better` |
| `diskio-sequential-write-32k:writeSequentialFile` | `26.888 ms/op` | `26.248 ms/op` | `-2.38%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `31.292 ms/op` | `31.766 ms/op` | `+1.51%` | `neutral` |

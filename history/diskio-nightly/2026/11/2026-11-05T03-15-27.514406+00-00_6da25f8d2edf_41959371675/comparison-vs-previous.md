# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `84.755 ms/op` | `92.805 ms/op` | `+9.50%` | `better` |
| `diskio-sequential-read-32k:readSequentialFile` | `57.201 ms/op` | `58.730 ms/op` | `+2.67%` | `neutral` |
| `diskio-sequential-read-4k:readSequentialFile` | `64.476 ms/op` | `66.789 ms/op` | `+3.59%` | `better` |
| `diskio-sequential-write-1k:writeSequentialFile` | `38.748 ms/op` | `41.036 ms/op` | `+5.91%` | `better` |
| `diskio-sequential-write-32k:writeSequentialFile` | `27.103 ms/op` | `26.559 ms/op` | `-2.00%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `31.451 ms/op` | `32.047 ms/op` | `+1.90%` | `neutral` |

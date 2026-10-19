# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `93.973 ms/op` | `83.677 ms/op` | `-10.96%` | `worse` |
| `diskio-sequential-read-32k:readSequentialFile` | `61.057 ms/op` | `53.067 ms/op` | `-13.09%` | `worse` |
| `diskio-sequential-read-4k:readSequentialFile` | `66.133 ms/op` | `63.076 ms/op` | `-4.62%` | `warning` |
| `diskio-sequential-write-1k:writeSequentialFile` | `40.278 ms/op` | `38.759 ms/op` | `-3.77%` | `warning` |
| `diskio-sequential-write-32k:writeSequentialFile` | `26.122 ms/op` | `26.851 ms/op` | `+2.79%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `31.595 ms/op` | `31.133 ms/op` | `-1.46%` | `neutral` |

# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `93.807 ms/op` | `82.002 ms/op` | `-12.58%` | `worse` |
| `diskio-sequential-read-32k:readSequentialFile` | `68.529 ms/op` | `64.077 ms/op` | `-6.50%` | `warning` |
| `diskio-sequential-read-4k:readSequentialFile` | `67.696 ms/op` | `67.841 ms/op` | `+0.21%` | `neutral` |
| `diskio-sequential-write-1k:writeSequentialFile` | `40.542 ms/op` | `38.040 ms/op` | `-6.17%` | `warning` |
| `diskio-sequential-write-32k:writeSequentialFile` | `26.248 ms/op` | `26.136 ms/op` | `-0.43%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `31.766 ms/op` | `30.417 ms/op` | `-4.25%` | `warning` |

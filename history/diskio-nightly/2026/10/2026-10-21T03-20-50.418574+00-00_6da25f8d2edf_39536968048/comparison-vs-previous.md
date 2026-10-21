# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `83.302 ms/op` | `83.086 ms/op` | `-0.26%` | `neutral` |
| `diskio-sequential-read-32k:readSequentialFile` | `56.510 ms/op` | `52.911 ms/op` | `-6.37%` | `warning` |
| `diskio-sequential-read-4k:readSequentialFile` | `64.008 ms/op` | `61.728 ms/op` | `-3.56%` | `warning` |
| `diskio-sequential-write-1k:writeSequentialFile` | `39.694 ms/op` | `38.283 ms/op` | `-3.55%` | `warning` |
| `diskio-sequential-write-32k:writeSequentialFile` | `27.124 ms/op` | `26.254 ms/op` | `-3.21%` | `warning` |
| `diskio-sequential-write-4k:writeSequentialFile` | `31.570 ms/op` | `30.410 ms/op` | `-3.67%` | `warning` |

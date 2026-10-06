# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `83.549 ms/op` | `81.281 ms/op` | `-2.72%` | `neutral` |
| `diskio-sequential-read-32k:readSequentialFile` | `67.766 ms/op` | `53.124 ms/op` | `-21.61%` | `worse` |
| `diskio-sequential-read-4k:readSequentialFile` | `61.979 ms/op` | `61.551 ms/op` | `-0.69%` | `neutral` |
| `diskio-sequential-write-1k:writeSequentialFile` | `38.977 ms/op` | `38.716 ms/op` | `-0.67%` | `neutral` |
| `diskio-sequential-write-32k:writeSequentialFile` | `26.497 ms/op` | `26.657 ms/op` | `+0.60%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `31.042 ms/op` | `30.812 ms/op` | `-0.74%` | `neutral` |

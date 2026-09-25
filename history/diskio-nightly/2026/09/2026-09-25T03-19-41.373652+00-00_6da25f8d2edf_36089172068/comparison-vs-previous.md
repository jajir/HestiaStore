# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `92.741 ms/op` | `82.289 ms/op` | `-11.27%` | `worse` |
| `diskio-sequential-read-32k:readSequentialFile` | `62.078 ms/op` | `56.382 ms/op` | `-9.18%` | `worse` |
| `diskio-sequential-read-4k:readSequentialFile` | `67.304 ms/op` | `60.300 ms/op` | `-10.41%` | `worse` |
| `diskio-sequential-write-1k:writeSequentialFile` | `40.366 ms/op` | `39.585 ms/op` | `-1.93%` | `neutral` |
| `diskio-sequential-write-32k:writeSequentialFile` | `25.906 ms/op` | `26.445 ms/op` | `+2.08%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `31.636 ms/op` | `31.678 ms/op` | `+0.13%` | `neutral` |

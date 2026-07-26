# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `96593cd49b374380016362f1fc4d5bacdf709832`
- Candidate SHA: `96593cd49b374380016362f1fc4d5bacdf709832`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `84.010 ms/op` | `85.997 ms/op` | `+2.37%` | `neutral` |
| `diskio-sequential-read-32k:readSequentialFile` | `55.112 ms/op` | `57.417 ms/op` | `+4.18%` | `better` |
| `diskio-sequential-read-4k:readSequentialFile` | `61.418 ms/op` | `68.256 ms/op` | `+11.13%` | `better` |
| `diskio-sequential-write-1k:writeSequentialFile` | `38.615 ms/op` | `38.131 ms/op` | `-1.25%` | `neutral` |
| `diskio-sequential-write-32k:writeSequentialFile` | `25.592 ms/op` | `25.387 ms/op` | `-0.80%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `32.783 ms/op` | `30.847 ms/op` | `-5.90%` | `warning` |

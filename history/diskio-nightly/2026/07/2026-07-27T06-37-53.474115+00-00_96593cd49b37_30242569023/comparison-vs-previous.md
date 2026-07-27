# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `96593cd49b374380016362f1fc4d5bacdf709832`
- Candidate SHA: `96593cd49b374380016362f1fc4d5bacdf709832`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `85.997 ms/op` | `83.752 ms/op` | `-2.61%` | `neutral` |
| `diskio-sequential-read-32k:readSequentialFile` | `57.417 ms/op` | `56.632 ms/op` | `-1.37%` | `neutral` |
| `diskio-sequential-read-4k:readSequentialFile` | `68.256 ms/op` | `62.146 ms/op` | `-8.95%` | `worse` |
| `diskio-sequential-write-1k:writeSequentialFile` | `38.131 ms/op` | `38.293 ms/op` | `+0.43%` | `neutral` |
| `diskio-sequential-write-32k:writeSequentialFile` | `25.387 ms/op` | `25.735 ms/op` | `+1.37%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `30.847 ms/op` | `30.236 ms/op` | `-1.98%` | `neutral` |

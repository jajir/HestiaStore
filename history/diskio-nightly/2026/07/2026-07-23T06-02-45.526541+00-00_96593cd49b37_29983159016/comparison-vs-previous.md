# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `dce1fbc911dbad09d8df685697a7c8b43069ed09`
- Candidate SHA: `96593cd49b374380016362f1fc4d5bacdf709832`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `85.563 ms/op` | `89.237 ms/op` | `+4.29%` | `better` |
| `diskio-sequential-read-32k:readSequentialFile` | `56.836 ms/op` | `56.064 ms/op` | `-1.36%` | `neutral` |
| `diskio-sequential-read-4k:readSequentialFile` | `63.065 ms/op` | `60.791 ms/op` | `-3.61%` | `warning` |
| `diskio-sequential-write-1k:writeSequentialFile` | `38.368 ms/op` | `38.093 ms/op` | `-0.72%` | `neutral` |
| `diskio-sequential-write-32k:writeSequentialFile` | `25.886 ms/op` | `25.248 ms/op` | `-2.47%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `30.672 ms/op` | `30.048 ms/op` | `-2.04%` | `neutral` |

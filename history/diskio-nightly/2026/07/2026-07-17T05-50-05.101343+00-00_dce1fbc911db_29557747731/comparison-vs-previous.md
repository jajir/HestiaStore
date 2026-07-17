# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `8b88b599997497df9b956d4be577a2b7424cedcb`
- Candidate SHA: `dce1fbc911dbad09d8df685697a7c8b43069ed09`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `97.746 ms/op` | `83.812 ms/op` | `-14.25%` | `worse` |
| `diskio-sequential-read-32k:readSequentialFile` | `59.875 ms/op` | `58.825 ms/op` | `-1.75%` | `neutral` |
| `diskio-sequential-read-4k:readSequentialFile` | `70.385 ms/op` | `72.342 ms/op` | `+2.78%` | `neutral` |
| `diskio-sequential-write-1k:writeSequentialFile` | `41.814 ms/op` | `38.748 ms/op` | `-7.33%` | `worse` |
| `diskio-sequential-write-32k:writeSequentialFile` | `26.073 ms/op` | `26.053 ms/op` | `-0.08%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `32.398 ms/op` | `31.289 ms/op` | `-3.42%` | `warning` |

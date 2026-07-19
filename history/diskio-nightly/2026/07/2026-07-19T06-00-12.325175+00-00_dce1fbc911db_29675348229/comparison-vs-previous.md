# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `dce1fbc911dbad09d8df685697a7c8b43069ed09`
- Candidate SHA: `dce1fbc911dbad09d8df685697a7c8b43069ed09`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `68.862 ms/op` | `77.161 ms/op` | `+12.05%` | `better` |
| `diskio-sequential-read-32k:readSequentialFile` | `53.212 ms/op` | `48.173 ms/op` | `-9.47%` | `worse` |
| `diskio-sequential-read-4k:readSequentialFile` | `53.700 ms/op` | `54.599 ms/op` | `+1.67%` | `neutral` |
| `diskio-sequential-write-1k:writeSequentialFile` | `64.227 ms/op` | `58.300 ms/op` | `-9.23%` | `worse` |
| `diskio-sequential-write-32k:writeSequentialFile` | `80.036 ms/op` | `54.306 ms/op` | `-32.15%` | `worse` |
| `diskio-sequential-write-4k:writeSequentialFile` | `55.843 ms/op` | `53.171 ms/op` | `-4.79%` | `warning` |

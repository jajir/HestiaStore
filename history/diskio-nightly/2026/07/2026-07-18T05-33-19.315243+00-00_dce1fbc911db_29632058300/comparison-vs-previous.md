# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `dce1fbc911dbad09d8df685697a7c8b43069ed09`
- Candidate SHA: `dce1fbc911dbad09d8df685697a7c8b43069ed09`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `83.812 ms/op` | `68.862 ms/op` | `-17.84%` | `worse` |
| `diskio-sequential-read-32k:readSequentialFile` | `58.825 ms/op` | `53.212 ms/op` | `-9.54%` | `worse` |
| `diskio-sequential-read-4k:readSequentialFile` | `72.342 ms/op` | `53.700 ms/op` | `-25.77%` | `worse` |
| `diskio-sequential-write-1k:writeSequentialFile` | `38.748 ms/op` | `64.227 ms/op` | `+65.76%` | `better` |
| `diskio-sequential-write-32k:writeSequentialFile` | `26.053 ms/op` | `80.036 ms/op` | `+207.20%` | `better` |
| `diskio-sequential-write-4k:writeSequentialFile` | `31.289 ms/op` | `55.843 ms/op` | `+78.48%` | `better` |

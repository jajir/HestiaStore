# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `dce1fbc911dbad09d8df685697a7c8b43069ed09`
- Candidate SHA: `dce1fbc911dbad09d8df685697a7c8b43069ed09`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `77.161 ms/op` | `99.699 ms/op` | `+29.21%` | `better` |
| `diskio-sequential-read-32k:readSequentialFile` | `48.173 ms/op` | `58.720 ms/op` | `+21.89%` | `better` |
| `diskio-sequential-read-4k:readSequentialFile` | `54.599 ms/op` | `77.440 ms/op` | `+41.83%` | `better` |
| `diskio-sequential-write-1k:writeSequentialFile` | `58.300 ms/op` | `41.462 ms/op` | `-28.88%` | `worse` |
| `diskio-sequential-write-32k:writeSequentialFile` | `54.306 ms/op` | `26.432 ms/op` | `-51.33%` | `worse` |
| `diskio-sequential-write-4k:writeSequentialFile` | `53.171 ms/op` | `32.108 ms/op` | `-39.61%` | `worse` |

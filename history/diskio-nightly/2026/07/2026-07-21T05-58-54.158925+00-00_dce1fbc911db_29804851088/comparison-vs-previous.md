# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `dce1fbc911dbad09d8df685697a7c8b43069ed09`
- Candidate SHA: `dce1fbc911dbad09d8df685697a7c8b43069ed09`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `99.699 ms/op` | `99.736 ms/op` | `+0.04%` | `neutral` |
| `diskio-sequential-read-32k:readSequentialFile` | `58.720 ms/op` | `69.623 ms/op` | `+18.57%` | `better` |
| `diskio-sequential-read-4k:readSequentialFile` | `77.440 ms/op` | `68.472 ms/op` | `-11.58%` | `worse` |
| `diskio-sequential-write-1k:writeSequentialFile` | `41.462 ms/op` | `42.352 ms/op` | `+2.15%` | `neutral` |
| `diskio-sequential-write-32k:writeSequentialFile` | `26.432 ms/op` | `26.694 ms/op` | `+0.99%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `32.108 ms/op` | `32.392 ms/op` | `+0.88%` | `neutral` |

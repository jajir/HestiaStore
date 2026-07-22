# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `dce1fbc911dbad09d8df685697a7c8b43069ed09`
- Candidate SHA: `dce1fbc911dbad09d8df685697a7c8b43069ed09`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `99.736 ms/op` | `85.563 ms/op` | `-14.21%` | `worse` |
| `diskio-sequential-read-32k:readSequentialFile` | `69.623 ms/op` | `56.836 ms/op` | `-18.37%` | `worse` |
| `diskio-sequential-read-4k:readSequentialFile` | `68.472 ms/op` | `63.065 ms/op` | `-7.90%` | `worse` |
| `diskio-sequential-write-1k:writeSequentialFile` | `42.352 ms/op` | `38.368 ms/op` | `-9.41%` | `worse` |
| `diskio-sequential-write-32k:writeSequentialFile` | `26.694 ms/op` | `25.886 ms/op` | `-3.03%` | `warning` |
| `diskio-sequential-write-4k:writeSequentialFile` | `32.392 ms/op` | `30.672 ms/op` | `-5.31%` | `warning` |

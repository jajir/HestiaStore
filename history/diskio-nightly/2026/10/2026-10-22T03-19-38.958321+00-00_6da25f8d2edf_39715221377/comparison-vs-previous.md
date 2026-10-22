# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `83.086 ms/op` | `81.342 ms/op` | `-2.10%` | `neutral` |
| `diskio-sequential-read-32k:readSequentialFile` | `52.911 ms/op` | `71.292 ms/op` | `+34.74%` | `better` |
| `diskio-sequential-read-4k:readSequentialFile` | `61.728 ms/op` | `75.815 ms/op` | `+22.82%` | `better` |
| `diskio-sequential-write-1k:writeSequentialFile` | `38.283 ms/op` | `40.348 ms/op` | `+5.39%` | `better` |
| `diskio-sequential-write-32k:writeSequentialFile` | `26.254 ms/op` | `35.696 ms/op` | `+35.96%` | `better` |
| `diskio-sequential-write-4k:writeSequentialFile` | `30.410 ms/op` | `36.945 ms/op` | `+21.49%` | `better` |

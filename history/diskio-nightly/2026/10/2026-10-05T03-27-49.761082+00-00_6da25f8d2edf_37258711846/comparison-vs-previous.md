# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `64.279 ms/op` | `83.549 ms/op` | `+29.98%` | `better` |
| `diskio-sequential-read-32k:readSequentialFile` | `48.730 ms/op` | `67.766 ms/op` | `+39.06%` | `better` |
| `diskio-sequential-read-4k:readSequentialFile` | `51.347 ms/op` | `61.979 ms/op` | `+20.71%` | `better` |
| `diskio-sequential-write-1k:writeSequentialFile` | `35.425 ms/op` | `38.977 ms/op` | `+10.03%` | `better` |
| `diskio-sequential-write-32k:writeSequentialFile` | `32.882 ms/op` | `26.497 ms/op` | `-19.42%` | `worse` |
| `diskio-sequential-write-4k:writeSequentialFile` | `37.316 ms/op` | `31.042 ms/op` | `-16.81%` | `worse` |

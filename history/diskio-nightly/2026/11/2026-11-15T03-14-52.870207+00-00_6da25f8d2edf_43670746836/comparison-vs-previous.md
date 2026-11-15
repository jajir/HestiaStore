# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `80.889 ms/op` | `67.866 ms/op` | `-16.10%` | `worse` |
| `diskio-sequential-read-32k:readSequentialFile` | `56.320 ms/op` | `57.461 ms/op` | `+2.03%` | `neutral` |
| `diskio-sequential-read-4k:readSequentialFile` | `61.174 ms/op` | `60.291 ms/op` | `-1.44%` | `neutral` |
| `diskio-sequential-write-1k:writeSequentialFile` | `38.532 ms/op` | `31.008 ms/op` | `-19.53%` | `worse` |
| `diskio-sequential-write-32k:writeSequentialFile` | `26.520 ms/op` | `39.067 ms/op` | `+47.31%` | `better` |
| `diskio-sequential-write-4k:writeSequentialFile` | `30.857 ms/op` | `33.051 ms/op` | `+7.11%` | `better` |

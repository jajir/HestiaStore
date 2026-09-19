# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `82.646 ms/op` | `73.837 ms/op` | `-10.66%` | `worse` |
| `diskio-sequential-read-32k:readSequentialFile` | `62.729 ms/op` | `54.667 ms/op` | `-12.85%` | `worse` |
| `diskio-sequential-read-4k:readSequentialFile` | `61.947 ms/op` | `55.888 ms/op` | `-9.78%` | `worse` |
| `diskio-sequential-write-1k:writeSequentialFile` | `38.343 ms/op` | `54.104 ms/op` | `+41.10%` | `better` |
| `diskio-sequential-write-32k:writeSequentialFile` | `25.484 ms/op` | `55.053 ms/op` | `+116.03%` | `better` |
| `diskio-sequential-write-4k:writeSequentialFile` | `30.123 ms/op` | `50.604 ms/op` | `+67.99%` | `better` |

# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `83.947 ms/op` | `75.972 ms/op` | `-9.50%` | `worse` |
| `diskio-sequential-read-32k:readSequentialFile` | `56.656 ms/op` | `48.554 ms/op` | `-14.30%` | `worse` |
| `diskio-sequential-read-4k:readSequentialFile` | `62.742 ms/op` | `55.147 ms/op` | `-12.10%` | `worse` |
| `diskio-sequential-write-1k:writeSequentialFile` | `39.163 ms/op` | `35.800 ms/op` | `-8.59%` | `worse` |
| `diskio-sequential-write-32k:writeSequentialFile` | `26.794 ms/op` | `33.688 ms/op` | `+25.73%` | `better` |
| `diskio-sequential-write-4k:writeSequentialFile` | `31.224 ms/op` | `46.354 ms/op` | `+48.46%` | `better` |

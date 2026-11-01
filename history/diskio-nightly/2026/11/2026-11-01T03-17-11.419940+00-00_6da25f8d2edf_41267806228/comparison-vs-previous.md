# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `85.386 ms/op` | `75.638 ms/op` | `-11.42%` | `worse` |
| `diskio-sequential-read-32k:readSequentialFile` | `56.113 ms/op` | `46.836 ms/op` | `-16.53%` | `worse` |
| `diskio-sequential-read-4k:readSequentialFile` | `60.988 ms/op` | `55.584 ms/op` | `-8.86%` | `worse` |
| `diskio-sequential-write-1k:writeSequentialFile` | `38.332 ms/op` | `36.876 ms/op` | `-3.80%` | `warning` |
| `diskio-sequential-write-32k:writeSequentialFile` | `26.521 ms/op` | `32.902 ms/op` | `+24.06%` | `better` |
| `diskio-sequential-write-4k:writeSequentialFile` | `31.438 ms/op` | `46.705 ms/op` | `+48.57%` | `better` |

# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `75.638 ms/op` | `83.028 ms/op` | `+9.77%` | `better` |
| `diskio-sequential-read-32k:readSequentialFile` | `46.836 ms/op` | `56.865 ms/op` | `+21.41%` | `better` |
| `diskio-sequential-read-4k:readSequentialFile` | `55.584 ms/op` | `61.110 ms/op` | `+9.94%` | `better` |
| `diskio-sequential-write-1k:writeSequentialFile` | `36.876 ms/op` | `37.581 ms/op` | `+1.91%` | `neutral` |
| `diskio-sequential-write-32k:writeSequentialFile` | `32.902 ms/op` | `26.509 ms/op` | `-19.43%` | `worse` |
| `diskio-sequential-write-4k:writeSequentialFile` | `46.705 ms/op` | `30.581 ms/op` | `-34.52%` | `worse` |

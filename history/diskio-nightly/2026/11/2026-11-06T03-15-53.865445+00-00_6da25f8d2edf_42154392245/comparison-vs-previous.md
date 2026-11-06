# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `92.805 ms/op` | `83.947 ms/op` | `-9.55%` | `worse` |
| `diskio-sequential-read-32k:readSequentialFile` | `58.730 ms/op` | `56.656 ms/op` | `-3.53%` | `warning` |
| `diskio-sequential-read-4k:readSequentialFile` | `66.789 ms/op` | `62.742 ms/op` | `-6.06%` | `warning` |
| `diskio-sequential-write-1k:writeSequentialFile` | `41.036 ms/op` | `39.163 ms/op` | `-4.57%` | `warning` |
| `diskio-sequential-write-32k:writeSequentialFile` | `26.559 ms/op` | `26.794 ms/op` | `+0.88%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `32.047 ms/op` | `31.224 ms/op` | `-2.57%` | `neutral` |

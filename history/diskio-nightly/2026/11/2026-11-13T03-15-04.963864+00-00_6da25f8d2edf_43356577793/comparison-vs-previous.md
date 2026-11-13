# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `73.182 ms/op` | `67.922 ms/op` | `-7.19%` | `worse` |
| `diskio-sequential-read-32k:readSequentialFile` | `50.308 ms/op` | `46.788 ms/op` | `-7.00%` | `warning` |
| `diskio-sequential-read-4k:readSequentialFile` | `56.788 ms/op` | `50.639 ms/op` | `-10.83%` | `worse` |
| `diskio-sequential-write-1k:writeSequentialFile` | `36.195 ms/op` | `35.047 ms/op` | `-3.17%` | `warning` |
| `diskio-sequential-write-32k:writeSequentialFile` | `35.684 ms/op` | `35.340 ms/op` | `-0.96%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `45.667 ms/op` | `34.670 ms/op` | `-24.08%` | `worse` |

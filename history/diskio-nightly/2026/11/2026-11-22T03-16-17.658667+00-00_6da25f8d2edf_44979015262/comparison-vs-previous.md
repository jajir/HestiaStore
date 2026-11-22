# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `88.799 ms/op` | `67.577 ms/op` | `-23.90%` | `worse` |
| `diskio-sequential-read-32k:readSequentialFile` | `76.657 ms/op` | `54.823 ms/op` | `-28.48%` | `worse` |
| `diskio-sequential-read-4k:readSequentialFile` | `79.595 ms/op` | `58.176 ms/op` | `-26.91%` | `worse` |
| `diskio-sequential-write-1k:writeSequentialFile` | `57.933 ms/op` | `30.495 ms/op` | `-47.36%` | `worse` |
| `diskio-sequential-write-32k:writeSequentialFile` | `41.076 ms/op` | `38.982 ms/op` | `-5.10%` | `warning` |
| `diskio-sequential-write-4k:writeSequentialFile` | `42.946 ms/op` | `34.501 ms/op` | `-19.66%` | `worse` |

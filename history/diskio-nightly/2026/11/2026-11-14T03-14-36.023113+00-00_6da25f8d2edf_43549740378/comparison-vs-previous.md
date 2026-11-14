# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `67.922 ms/op` | `80.889 ms/op` | `+19.09%` | `better` |
| `diskio-sequential-read-32k:readSequentialFile` | `46.788 ms/op` | `56.320 ms/op` | `+20.37%` | `better` |
| `diskio-sequential-read-4k:readSequentialFile` | `50.639 ms/op` | `61.174 ms/op` | `+20.81%` | `better` |
| `diskio-sequential-write-1k:writeSequentialFile` | `35.047 ms/op` | `38.532 ms/op` | `+9.94%` | `better` |
| `diskio-sequential-write-32k:writeSequentialFile` | `35.340 ms/op` | `26.520 ms/op` | `-24.96%` | `worse` |
| `diskio-sequential-write-4k:writeSequentialFile` | `34.670 ms/op` | `30.857 ms/op` | `-11.00%` | `worse` |

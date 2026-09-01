# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Candidate SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `91.559 ms/op` | `80.580 ms/op` | `-11.99%` | `worse` |
| `diskio-sequential-read-32k:readSequentialFile` | `60.912 ms/op` | `68.568 ms/op` | `+12.57%` | `better` |
| `diskio-sequential-read-4k:readSequentialFile` | `68.156 ms/op` | `80.374 ms/op` | `+17.93%` | `better` |
| `diskio-sequential-write-1k:writeSequentialFile` | `41.240 ms/op` | `38.896 ms/op` | `-5.69%` | `warning` |
| `diskio-sequential-write-32k:writeSequentialFile` | `26.275 ms/op` | `34.161 ms/op` | `+30.01%` | `better` |
| `diskio-sequential-write-4k:writeSequentialFile` | `31.931 ms/op` | `35.416 ms/op` | `+10.91%` | `better` |

# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Candidate SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `80.580 ms/op` | `89.633 ms/op` | `+11.23%` | `better` |
| `diskio-sequential-read-32k:readSequentialFile` | `68.568 ms/op` | `59.610 ms/op` | `-13.06%` | `worse` |
| `diskio-sequential-read-4k:readSequentialFile` | `80.374 ms/op` | `68.841 ms/op` | `-14.35%` | `worse` |
| `diskio-sequential-write-1k:writeSequentialFile` | `38.896 ms/op` | `40.059 ms/op` | `+2.99%` | `neutral` |
| `diskio-sequential-write-32k:writeSequentialFile` | `34.161 ms/op` | `26.057 ms/op` | `-23.72%` | `worse` |
| `diskio-sequential-write-4k:writeSequentialFile` | `35.416 ms/op` | `31.888 ms/op` | `-9.96%` | `worse` |

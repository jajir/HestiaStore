# Benchmark Comparison

- Profile: `diskio-nightly`
- Baseline SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Candidate SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `diskio-sequential-read-1k:readSequentialFile` | `90.803 ms/op` | `91.559 ms/op` | `+0.83%` | `neutral` |
| `diskio-sequential-read-32k:readSequentialFile` | `55.342 ms/op` | `60.912 ms/op` | `+10.06%` | `better` |
| `diskio-sequential-read-4k:readSequentialFile` | `62.243 ms/op` | `68.156 ms/op` | `+9.50%` | `better` |
| `diskio-sequential-write-1k:writeSequentialFile` | `39.060 ms/op` | `41.240 ms/op` | `+5.58%` | `better` |
| `diskio-sequential-write-32k:writeSequentialFile` | `25.600 ms/op` | `26.275 ms/op` | `+2.64%` | `neutral` |
| `diskio-sequential-write-4k:writeSequentialFile` | `30.908 ms/op` | `31.931 ms/op` | `+3.31%` | `better` |

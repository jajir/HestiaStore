# How to run JMH benchmarks

Follow this steps:
* Go to `jmh-benchmarks`
* Create pacakge containing all benchmarks data
* Execute it, with temp directory in `target` directory

```bash
cd jmh-benchmarks
mvn package
java -Ddir=./target/ -jar target/jmh-benchmarks.jar
```

result could look like:

```
Benchmark                                        Mode  Cnt  Score   Error  Units
FileReadingBenchmark.testReadDataWithBuffer16KB  avgt   25  0.941 ± 0.010  ms/op
FileReadingBenchmark.testReadDataWithBuffer1KB   avgt   25  1.405 ± 0.090  ms/op
FileReadingBenchmark.testReadDataWithBuffer2KB   avgt   25  1.117 ± 0.007  ms/op
FileReadingBenchmark.testReadDataWithBuffer4KB   avgt   25  1.028 ± 0.008  ms/op
FileReadingBenchmark.testReadDataWithBuffer8KB   avgt   25  0.964 ± 0.005  ms/op
```

## Development

Mockito requires reflective access to non-public parts in a Java module. It could be manually open by passing following parameter as jvm parameter:

```
--add-opens=java.base/java.lang=ALL-UNNAMED
```

## How to get segment disk size

On apple try:

```
diskutil  info /Volumes/LaCie
```

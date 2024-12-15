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

Specific JMH benchmark class could be run;

```bash
java -Ddir=./target/ -jar target/jmh-benchmarks.jar SegmentSearchBenchmark
```

result could look like:

```
Benchmark                                             Mode  Cnt    Score      Error  Units
SequentialFileReadingBenchmark.test_with_buffer_01KB  avgt    4   70.747 ±   42.480  ms/op
SequentialFileReadingBenchmark.test_with_buffer_02KB  avgt    4   60.009 ±   52.899  ms/op
SequentialFileReadingBenchmark.test_with_buffer_04KB  avgt    4   51.254 ±   30.112  ms/op
SequentialFileReadingBenchmark.test_with_buffer_08KB  avgt    4   48.600 ±   28.892  ms/op
SequentialFileReadingBenchmark.test_with_buffer_16KB  avgt    4   48.471 ±   25.665  ms/op
SequentialFileReadingBenchmark.test_with_buffer_32KB  avgt    4   45.256 ±   24.986  ms/op
SequentialFileReadingBenchmark.test_with_buffer_64KB  avgt    4   45.204 ±   24.867  ms/op
SequentialFileWritingBenchmark.test_with_buffer_01KB  avgt    4  238.075 ±   75.311  ms/op
SequentialFileWritingBenchmark.test_with_buffer_02KB  avgt    4  271.272 ±   64.747  ms/op
SequentialFileWritingBenchmark.test_with_buffer_04KB  avgt    4  276.001 ±   45.815  ms/op
SequentialFileWritingBenchmark.test_with_buffer_08KB  avgt    4  352.189 ± 1140.814  ms/op
SequentialFileWritingBenchmark.test_with_buffer_16KB  avgt    4  258.806 ±   44.693  ms/op
SequentialFileWritingBenchmark.test_with_buffer_32KB  avgt    4  276.246 ±  135.019  ms/op
SequentialFileWritingBenchmark.test_with_buffer_64KB  avgt    4  275.944 ±  128.835  ms/op
```

### Possible problems

Generally JMH is quite fragile and generates. Usually help rebuild prject and start again as described above.

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

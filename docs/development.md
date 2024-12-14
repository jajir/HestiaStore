#How to run JMH benchmarks

Follow this steps:
* Go to `jmh-benchmarks`
* Create pacakge containing all benchmarks data
* Execute it, with temp directory in `target` directory

```bash
cd jmh-benchmarks
mvn package
java -Ddir=./target/ -jar target/jmh-benchmarks.jar
```

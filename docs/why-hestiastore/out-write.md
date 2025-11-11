# 📊 HestiaStore Benchmark Results

## 🧪 Test Conditions

- Every benchmark in the plain-load suite runs inside the same controlled JVM environment with identical JVM flags and hardware resources. Runs start by wiping the working directory supplied through the `dir` system property, so each trial writes into a fresh, empty location.
- Execution stays single-threaded from warm-up through measurement. The test focuses purely on how quickly one writer can push key/value pairs into the storage engine without any coordination overhead from additional threads.
- Warm-up phases fill the database as aggressively as possible for several 20-second stretches. This stage is meant to trigger JIT compilation, populate caches, and let LevelDB settle into steady-state behaviour before any numbers are recorded.
- Measurement phases repeat the same single-threaded write loop. Throughput is observed over multiple 20-second intervals to capture stable, sustained insert performance rather than a burst.
- Each write operation uses a deterministic pseudo-random long (seed `324432L`) to generate a unique hash string via `HashDataProvider`. The payload is the constant text `"opice skace po stromech"`, so variability comes exclusively from the changing keys.
- After measurements complete, the map is closed and the directory remains available for inspection. The log records how many keys were created, providing a quick sanity check that the run processed the expected volume.
- Test was performed at Mac mini 2024, 16 GB, macOS 15.6.1 (24G90).


## 🏁 Benchmark Results

| Engine       | Score [ops/s]     | ScoreError | Confidence Interval [ops/s] | Occupied space | CPU Usage |
|:--------------|-----------------:|-----------:|-----------------------------:|---------------:|---------:|
| ChronicleMap |           5 954 |     1 765 | 4 189 .. 7 719              | 20.54 GB       | 7%         |
| H2           |          13 458 |     5 144 | 8 314 .. 18 601             | 8 KB           | 21%        |
| LevelDB      |          45 263 |    10 913 | 34 350 .. 56 176            | 1.4 GB         | 17%        |
| MapDB        |           2 946 |       326 | 2 620 .. 3 272              | 496 MB         | 14%        |
| RocksDB      |         305 712 |    78 929 | 226 783 .. 384 641          | 7.74 GB        | 6%         |

meaning of columns:

- Engine: name of the benchmarked engine (as derived from the JSON filename)
- Score [ops/s]: number of operations per second (higher is better)
- ScoreError: error margin of the score (lower is better). It's computed as `z * (stdev / sqrt(n)) where`
  - `z` is the z-score for the desired confidence level (1.96 for 95%)
  - `stdev` is the standard deviation of the measurements
  - `n` is the number of measurements
- Confidence Interval: 95% confidence interval of the score (lower and upper bound). This means that the true mean is likely between this interval of ops/sec. Negative values are possible if the error margin is larger than the score itself.
- Occupied space : amount of disk space occupied by the engine's data structures (lower is better). It is measured after flushing last data to disk.
- CPU Usage: average CPU usage during the benchmark (lower is better). Please note, that it includes all system processes, not only the benchmarked engine.

## 📄 Raw JSON Files

### results-write-ChronicleMap-my.json

```json
{
    "totalDirectorySize": 22049472512,
    "fileCount": 1,
    "usedMemoryBytes": 27947104,
    "cpuBefore": 603274000,
    "cpuAfter": 1097938000,
    "startTime": 261108533575583,
    "endTime": 261814614016416,
    "cpuUsage": 0.07005774008077877
}
```

### results-write-ChronicleMap.json

```json
[
    {
        "jmhVersion" : "1.37",
        "benchmark" : "org.hestiastore.index.benchmark.plainload.TestChronicleMap.write",
        "mode" : "thrpt",
        "threads" : 1,
        "forks" : 1,
        "jvm" : "/opt/homebrew/Cellar/openjdk@21/21.0.7/libexec/openjdk.jdk/Contents/Home/bin/java",
        "jvmArgs" : [
            "-Xmx10000m",
            "--add-opens=java.base/java.lang=ALL-UNNAMED",
            "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
            "--add-opens=java.base/java.io=ALL-UNNAMED",
            "--add-opens=java.base/java.nio=ALL-UNNAMED",
            "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
            "--add-opens=jdk.compiler/com.sun.tools.javac=ALL-UNNAMED",
            "--add-opens=jdk.compiler/com.sun.tools.javac.api=ALL-UNNAMED",
            "--add-opens=jdk.compiler/com.sun.tools.javac.code=ALL-UNNAMED",
            "--add-opens=jdk.compiler/com.sun.tools.javac.comp=ALL-UNNAMED",
            "--add-opens=jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED",
            "--add-opens=jdk.compiler/com.sun.tools.javac.main=ALL-UNNAMED",
            "--add-opens=jdk.compiler/com.sun.tools.javac.model=ALL-UNNAMED",
            "--add-opens=jdk.compiler/com.sun.tools.javac.parser=ALL-UNNAMED",
            "--add-opens=jdk.compiler/com.sun.tools.javac.processing=ALL-UNNAMED",
            "--add-opens=jdk.compiler/com.sun.tools.javac.util=ALL-UNNAMED",
            "-Ddir=/Volumes/ponrava/test-index",
            "-Dengine=ChronicleMap"
        ],
        "jdkVersion" : "21.0.7",
        "vmName" : "OpenJDK 64-Bit Server VM",
        "vmVersion" : "21.0.7",
        "warmupIterations" : 10,
        "warmupTime" : "20 s",
        "warmupBatchSize" : 1,
        "measurementIterations" : 25,
        "measurementTime" : "20 s",
        "measurementBatchSize" : 1,
        "primaryMetric" : {
            "score" : 5954.200373600311,
            "scoreError" : 1764.90521298729,
            "scoreConfidence" : [
                4189.295160613021,
                7719.105586587601
            ],
            "scorePercentiles" : {
                "0.0" : 3329.640674916557,
                "50.0" : 5243.630014833653,
                "90.0" : 10595.405308112338,
                "95.0" : 12253.872048619854,
                "99.0" : 12744.707905617659,
                "99.9" : 12744.707905617659,
                "99.99" : 12744.707905617659,
                "99.999" : 12744.707905617659,
                "99.9999" : 12744.707905617659,
                "100.0" : 12744.707905617659
            },
            "scoreUnit" : "ops/s",
            "rawData" : [
                [
                    11108.58838229165,
                    12744.707905617659,
                    10253.283258659461,
                    7641.776148398347,
                    7065.625130993228,
                    6245.862123216896,
                    5726.337056869982,
                    5546.963290190697,
                    5312.460633150494,
                    5243.630014833653,
                    5127.273627636177,
                    4443.333421250085,
                    4570.51338982323,
                    4905.02077335985,
                    5196.530108141268,
                    3329.640674916557,
                    3829.198465291546,
                    7600.50765102385,
                    6131.287347339458,
                    5543.996756565399,
                    4995.077322180827,
                    4772.012372724426,
                    3916.673020709464,
                    3514.748215845972,
                    4089.96224897762
                ]
            ]
        },
        "secondaryMetrics" : {
        }
    }
]



```

### results-write-H2-my.json

```json
{
  "totalDirectorySize" : 8192,
  "fileCount" : 1,
  "usedMemoryBytes" : 27133320,
  "cpuBefore" : 1035200000,
  "cpuAfter" : 2479448000,
  "startTime" : 258786636432958,
  "endTime" : 259488720650875,
  "cpuUsage" : 0.2057086547657931
}
```

### results-write-H2.json

```json
[
    {
        "jmhVersion" : "1.37",
        "benchmark" : "org.hestiastore.index.benchmark.plainload.TestH2.write",
        "mode" : "thrpt",
        "threads" : 1,
        "forks" : 1,
        "jvm" : "/opt/homebrew/Cellar/openjdk@21/21.0.7/libexec/openjdk.jdk/Contents/Home/bin/java",
        "jvmArgs" : [
            "-Xmx10000m",
            "-Ddir=/Volumes/ponrava/test-index",
            "-Dengine=H2"
        ],
        "jdkVersion" : "21.0.7",
        "vmName" : "OpenJDK 64-Bit Server VM",
        "vmVersion" : "21.0.7",
        "warmupIterations" : 10,
        "warmupTime" : "20 s",
        "warmupBatchSize" : 1,
        "measurementIterations" : 25,
        "measurementTime" : "20 s",
        "measurementBatchSize" : 1,
        "primaryMetric" : {
            "score" : 13457.521378452284,
            "scoreError" : 5143.803121401257,
            "scoreConfidence" : [
                8313.718257051027,
                18601.32449985354
            ],
            "scorePercentiles" : {
                "0.0" : 4492.791786534924,
                "50.0" : 13209.02178518997,
                "90.0" : 21853.77922605105,
                "95.0" : 29631.650534341064,
                "99.0" : 32749.29402436372,
                "99.9" : 32749.29402436372,
                "99.99" : 32749.29402436372,
                "99.999" : 32749.29402436372,
                "99.9999" : 32749.29402436372,
                "100.0" : 32749.29402436372
            },
            "scoreUnit" : "ops/s",
            "rawData" : [
                [
                    12623.654285480005,
                    11839.343622462286,
                    14436.728477425342,
                    13633.043439408131,
                    16157.467112454447,
                    6412.401044547184,
                    6265.687494095522,
                    18612.922946912684,
                    16219.969068239285,
                    12831.893972457165,
                    18859.80952879762,
                    4813.230127324451,
                    13209.02178518997,
                    22357.14905762156,
                    18073.648245485118,
                    8538.249998991463,
                    32749.29402436372,
                    18767.680908037615,
                    21518.199338337377,
                    8642.521511716302,
                    17435.121051959966,
                    7154.46408403396,
                    4670.397715531453,
                    4492.791786534924,
                    6123.3438338994565
                ]
            ]
        },
        "secondaryMetrics" : {
        }
    }
]



```

### results-write-HestiaStoreBasic-my.json

```json
{
  "totalDirectorySize" : 3889698398,
  "fileCount" : 38,
  "usedMemoryBytes" : 28606416,
  "cpuBefore" : 552320000,
  "cpuAfter" : 729480000,
  "startTime" : 1401225500145958,
  "endTime" : 1401225903588125,
  "cpuUsage" : 43.91211789222816
}
```

### results-write-HestiaStoreBasic.json

```json
[
]



```

### results-write-HestiaStoreCompress-my.json

```json
{
  "totalDirectorySize" : 3889698398,
  "fileCount" : 38,
  "usedMemoryBytes" : 28689648,
  "cpuBefore" : 529915000,
  "cpuAfter" : 697688000,
  "startTime" : 1401226232499375,
  "endTime" : 1401226604772458,
  "cpuUsage" : 45.067185263029074
}
```

### results-write-HestiaStoreCompress.json

```json
[
]



```

### results-write-LevelDB-my.json

```json
{
    "totalDirectorySize": 1508330468,
    "fileCount": 754,
    "usedMemoryBytes": 27954256,
    "cpuBefore": 1001444000,
    "cpuAfter": 2178345000,
    "startTime": 260193098308000,
    "endTime": 260895356813250,
    "cpuUsage": 0.1675880023099229
}
```

### results-write-LevelDB.json

```json
[
    {
        "jmhVersion" : "1.37",
        "benchmark" : "org.hestiastore.index.benchmark.plainload.TestLevelDB.write",
        "mode" : "thrpt",
        "threads" : 1,
        "forks" : 1,
        "jvm" : "/opt/homebrew/Cellar/openjdk@21/21.0.7/libexec/openjdk.jdk/Contents/Home/bin/java",
        "jvmArgs" : [
            "-Xmx10000m",
            "-Ddir=/Volumes/ponrava/test-index",
            "-Dengine=LevelDB"
        ],
        "jdkVersion" : "21.0.7",
        "vmName" : "OpenJDK 64-Bit Server VM",
        "vmVersion" : "21.0.7",
        "warmupIterations" : 10,
        "warmupTime" : "20 s",
        "warmupBatchSize" : 1,
        "measurementIterations" : 25,
        "measurementTime" : "20 s",
        "measurementBatchSize" : 1,
        "primaryMetric" : {
            "score" : 45262.57575204393,
            "scoreError" : 10913.06838787145,
            "scoreConfidence" : [
                34349.50736417248,
                56175.64413991538
            ],
            "scorePercentiles" : {
                "0.0" : 29965.68796327744,
                "50.0" : 44015.87124729542,
                "90.0" : 59895.29706634639,
                "95.0" : 73862.46532443774,
                "99.0" : 79758.10180257274,
                "99.9" : 79758.10180257274,
                "99.99" : 79758.10180257274,
                "99.999" : 79758.10180257274,
                "99.9999" : 79758.10180257274,
                "100.0" : 79758.10180257274
            },
            "scoreUnit" : "ops/s",
            "rawData" : [
                [
                    79758.10180257274,
                    39978.85200469439,
                    59754.841638050995,
                    30051.495747236888,
                    59710.167696545956,
                    30759.864807986865,
                    59261.071048612,
                    30156.92824167015,
                    45828.62436018362,
                    44015.87124729542,
                    30051.946454605757,
                    42438.257643466684,
                    47017.87854596809,
                    30308.896587398805,
                    59599.16125957037,
                    29965.68796327744,
                    30159.1527061617,
                    59564.58477647708,
                    30361.845682382078,
                    30347.55964289471,
                    59253.07772711731,
                    32183.0951788089,
                    57862.32461600187,
                    60105.98020878948,
                    53069.126213328724
                ]
            ]
        },
        "secondaryMetrics" : {
        }
    }
]



```

### results-write-MapDB-my.json

```json
{
  "totalDirectorySize" : 520093696,
  "fileCount" : 1,
  "usedMemoryBytes" : 27726856,
  "cpuBefore" : 1110727000,
  "cpuAfter" : 2088963000,
  "startTime" : 259489732026041,
  "endTime" : 260192350201750,
  "cpuUsage" : 0.13922725511802747
}
```

### results-write-MapDB.json

```json
[
    {
        "jmhVersion" : "1.37",
        "benchmark" : "org.hestiastore.index.benchmark.plainload.TestMapDB.write",
        "mode" : "thrpt",
        "threads" : 1,
        "forks" : 1,
        "jvm" : "/opt/homebrew/Cellar/openjdk@21/21.0.7/libexec/openjdk.jdk/Contents/Home/bin/java",
        "jvmArgs" : [
            "-Xmx10000m",
            "-Ddir=/Volumes/ponrava/test-index",
            "-Dengine=MapDB"
        ],
        "jdkVersion" : "21.0.7",
        "vmName" : "OpenJDK 64-Bit Server VM",
        "vmVersion" : "21.0.7",
        "warmupIterations" : 10,
        "warmupTime" : "20 s",
        "warmupBatchSize" : 1,
        "measurementIterations" : 25,
        "measurementTime" : "20 s",
        "measurementBatchSize" : 1,
        "primaryMetric" : {
            "score" : 2945.9818242698652,
            "scoreError" : 325.59501594188,
            "scoreConfidence" : [
                2620.3868083279854,
                3271.576840211745
            ],
            "scorePercentiles" : {
                "0.0" : 2272.884921159144,
                "50.0" : 2903.932835903283,
                "90.0" : 3477.3894675973625,
                "95.0" : 3980.5774803791674,
                "99.0" : 4191.114455891415,
                "99.9" : 4191.114455891415,
                "99.99" : 4191.114455891415,
                "99.999" : 4191.114455891415,
                "99.9999" : 4191.114455891415,
                "100.0" : 4191.114455891415
            },
            "scoreUnit" : "ops/s",
            "rawData" : [
                [
                    3355.129200069919,
                    4191.114455891415,
                    3489.3245375172587,
                    2438.7143275091894,
                    2579.0322123944043,
                    3197.038563619707,
                    3107.3043335466377,
                    3384.5420569152025,
                    3026.214223864194,
                    2955.4727465665437,
                    2938.449490331077,
                    2444.3275358242336,
                    2447.5359296109964,
                    2572.5750258119742,
                    2629.0978967032243,
                    2641.6700332863193,
                    2903.932835903283,
                    2821.317530851881,
                    2272.884921159144,
                    2668.589365207391,
                    3178.0194113129796,
                    3265.484517502896,
                    3469.4327543174313,
                    2897.2634039647874,
                    2775.078297064553
                ]
            ]
        },
        "secondaryMetrics" : {
        }
    }
]



```

### results-write-RocksDB-my.json

```json
{
  "totalDirectorySize" : 8306458361,
  "fileCount" : 143,
  "usedMemoryBytes" : 29881472,
  "cpuBefore" : 524178000,
  "cpuAfter" : 978766000,
  "startTime" : 256399236974125,
  "endTime" : 257100509331250,
  "cpuUsage" : 0.0648233165590143
}
```

### results-write-RocksDB.json

```json
[
    {
        "jmhVersion" : "1.37",
        "benchmark" : "org.hestiastore.index.benchmark.plainload.TestRocksDB.write",
        "mode" : "thrpt",
        "threads" : 1,
        "forks" : 1,
        "jvm" : "/opt/homebrew/Cellar/openjdk@21/21.0.7/libexec/openjdk.jdk/Contents/Home/bin/java",
        "jvmArgs" : [
            "-Ddir=/Volumes/ponrava/test-index",
            "-Dengine=RocksDB"
        ],
        "jdkVersion" : "21.0.7",
        "vmName" : "OpenJDK 64-Bit Server VM",
        "vmVersion" : "21.0.7",
        "warmupIterations" : 10,
        "warmupTime" : "20 s",
        "warmupBatchSize" : 1,
        "measurementIterations" : 25,
        "measurementTime" : "20 s",
        "measurementBatchSize" : 1,
        "primaryMetric" : {
            "score" : 305711.8304543295,
            "scoreError" : 78928.99893168075,
            "scoreConfidence" : [
                226782.83152264875,
                384640.82938601024
            ],
            "scorePercentiles" : {
                "0.0" : 64828.117457250206,
                "50.0" : 303051.53710396215,
                "90.0" : 440196.02218382317,
                "95.0" : 451632.3622033773,
                "99.0" : 452870.0724388766,
                "99.9" : 452870.0724388766,
                "99.99" : 452870.0724388766,
                "99.999" : 452870.0724388766,
                "99.9999" : 452870.0724388766,
                "100.0" : 452870.0724388766
            },
            "scoreUnit" : "ops/s",
            "rawData" : [
                [
                    452870.0724388766,
                    403842.93155871733,
                    294725.1949589473,
                    344879.7186297531,
                    252451.31054124268,
                    270367.7563971078,
                    265577.0790884009,
                    211979.36739862378,
                    70666.26824904919,
                    414677.1507219246,
                    64828.117457250206,
                    405398.2212372984,
                    386439.0605128443,
                    419727.7994319734,
                    434497.12253711926,
                    448744.3716538789,
                    235897.63938900485,
                    297712.0697930812,
                    303794.303173526,
                    360040.8711513881,
                    257499.74121652535,
                    303051.53710396215,
                    185201.4305266164,
                    303600.34376763576,
                    254326.28242349048
                ]
            ]
        },
        "secondaryMetrics" : {
        }
    }
]



```

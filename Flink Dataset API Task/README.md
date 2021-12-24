#  Flink Batch Processing

Task is accomplished using Java 8 with [gradle](https://gradle.org/) used as build tool.

Core logic is encapsulated in [TaskOne.java](src/main/java/TaskOne.java)
with [Application.java](src/main/java/Application.java) acting as driver and main entry point.



##  Tools 

* Java 8
* Gradle build tool
* IntelliJ IDEA
* Docker 
* Ubuntu


## Local:

```shell
./gradlew run --args "17274 /path_to_input/data/CA-GrQc.txt /path_to_project/data/result.csv"
```

## Docker:

```shell
docker run --rm -v $(pwd):/app --workdir="/app" --entrypoint /app/gradlew --name task_c gradle:6.7-jdk8 run --args "17274 /app/data/CA-GrQc.txt /app/data/result.csv"
```

Results will be written to [data](data) folder.

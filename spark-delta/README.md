##Spark Delta
Spark Delta is a project to get hand dirty with Spark and delta.io in order to evaluate solutions for modern Lake House Design.

### Implementation

The implementation include:
- Read data from CSV and read to delta table.
- Write to delta table with hive metastore.
- Read from Delta table with path and hive table.
- Read delta table as stream and sink data to postgres database with partitions 

### Data Pipeline
In general, the data pipeline will look like this
```
csv => spark transform  => delta table 1 => spark readStream => delta table 2 => postgres tables
```

## Prerequisite
- Install [spark 3.x](https://spark.apache.org/docs/latest/index.html)
- Install [scala 2.12.x](https://www.scala-lang.org/)
- Install [sbt](https://www.scala-sbt.org/) for scala build

## Build & Run
Project using scala version 2.13.7 and sbt 1.5.5.
- sbt build: `sbt clean package`
- sbt assembly: `sbt clean assembly`
- run spark submit:
```aidl
bin/spark-submit \
--master "local[*]" \
--class { path to scala-class } \
{ path to jars-file }
```
## Some screenshots

- Will be updated later

## Trouble shooting
Running `sbt clean assembly` with running unit test may cause to an error
```aidl
java.lang.OutOfMemoryError cannot be cast to xsbti.FullReload
```
It's possible because the limitation of memory size when running assembly `sbt` and `test` in the same JVM thread. Try to adjust the environment by this command below:
```aidl
$ export SBT_OPTS="-Xms1024M -Xmx4G -Xss2M -XX:MaxMetaspaceSize=2G"
```



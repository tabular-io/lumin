# lumin
Repository for code related to Lumin migration

## Resources
OpenTSDB Resources:

[HBase Schema](http://opentsdb.net/docs/build/html/user_guide/backends/hbase.html)

HBase Resources:
[CellUtil](https://hbase.apache.org/apidocs/org/apache/hadoop/hbase/CellUtil.html)

## Requirements

[Spark 3.3 w/ Hadoop 3](https://dlcdn.apache.org/spark/spark-3.3.0/spark-3.3.0-bin-hadoop3.tgz)

## Sample Data

Table `tsdb`: `s3://tabular-lumin/data/tsdb/d9b23544e7b34326a98410af7c69cf2f`

## Spark Setup

1. Download Spark from the link above
2. Build the `spark-hbase` project and shadowJar (`gradlew build shadowJar`)
3. Copy the shadow jar into the Spark `jars` directory
4. Run `./bin/spark-shell` from spark dir

## Spark Shell Exampless

```scala
import org.apache.hadoop.hbase.mapreduce.HFileInputFormat
import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.io.NullWritable
import com.google.common.primitives.Longs

var rdd = sc.newAPIHadoopFile("file:///Users/dweeks/workspace/lumin/data/", classOf[HFileInputFormat], classOf[NullWritable], classOf[Cell])

var cells = rdd.map(_._2) // second argument is the HBase Cell.  This is not serializable, so cannot be collected.
```

Accessing the row bytes:

```scala
var row = cells.map(_.getRowArray()).take(1)(0) 

// Access the timestamp bytes from the row array (see above OpenTSDB row format)
var ts = new Array[Byte](8)
System.arraycopy(row, 7, ts, 4, 4)
Longs.fromByteArray(ts)

// res114: Long = 1617552000 <- Timestamp hour per open tsdb spec
```


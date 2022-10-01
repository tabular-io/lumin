package lumin;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.mapreduce.HFileInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;

public class Main {

  public static void main(String[] args) {
    SparkConf conf =
        new SparkConf()
            .setMaster("local")
            .setAppName("tabular-analyzer")
            .set("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .set(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain");

    SparkSession spark = SparkSession.builder().config(conf).appName("lumin-import").getOrCreate();

    String dir = "s3://tabular-lumin/data/tsdb/";

    SparkContext ctx = spark.sparkContext();
    RDD<Row> rdd =
        ctx.newAPIHadoopFile(
                dir,
                HFileInputFormat.class,
                NullWritable.class,
                Cell.class,
                ctx.hadoopConfiguration())
            .toJavaRDD()
            .map(
                tuple -> {
                  Metric m = new Metric(tuple._2);
                  return RowFactory.create(
                      m.salt,
                      m.muid,
                      m.ts,
                      JavaConverters.mapAsScalaMap(m.tags),
                      m.qualifier,
                      m.value);
                })
            .rdd();

    StructType schema =
        StructType.fromDDL(
            "salt BINARY, metric_id BINARY, ts BINARY, tags MAP<BINARY, BINARY>, qualifier BINARY, value BINARY");

    Dataset<Row> df = spark.createDataset(rdd, RowEncoder.apply(schema));

    System.out.println("**** cnt: " + df.count());
  }
}

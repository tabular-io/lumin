package lumin;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

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
    String tsdbDir = "s3://tabular-lumin/data/tsdb/";
    String tsdbUidDir = "s3://tabular-lumin/data/tsdb-uid/";

    new Convert(spark, tsdbDir, tsdbUidDir).convert();
  }
}

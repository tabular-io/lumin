package io.tabular.tsdb.convert;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled("integration test")
public class ConvertTest {

  @Test
  public void testConvert() {
    SparkConf conf =
        new SparkConf()
            .setMaster("local[*]")
            .setAppName("tsdb-import")
            .set("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .set(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
            .set("spark.sql.catalog.tabular", "org.apache.iceberg.spark.SparkCatalog")
            .set("spark.sql.catalog.tabular.catalog-impl", "org.apache.iceberg.rest.RESTCatalog")
            .set("spark.sql.catalog.tabular.uri", "https://api.dev.tabulardata.io/ws")
            .set("spark.sql.catalog.tabular.credential", System.getenv("TABULAR_CREDS"))
            .set("spark.sql.defaultCatalog", "tabular")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

    SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
    String metricDir = "s3://tabular-lumin/data/tsdb";
    String uidDir = "s3://tabular-lumin/data/tsdb-uid";
    String outputTable = "default.lumin_metrics";

    ConvertOptions convertOptions =
        ConvertOptions.builder()
            .metricDir(metricDir)
            .uidDir(uidDir)
            .outputTable(outputTable)
            .idSize(4)
            .fanout(false)
            .build();

    new Convert(spark, convertOptions).convert();
  }

  @Test
  public void testConvertLegacy() {
    System.setProperty("aws.region", "us-east-1");
    System.setProperty("aws.profile", "lumin-access-role");
    SparkConf conf =
        new SparkConf()
            .setMaster("local[*]")
            .setAppName("tsdb-import")
            .set("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .set(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider")
            .set("spark.hadoop.fs.s3a.assumed.role.arn", System.getenv("TSDB_ARN"))
            .set(
                "spark.hadoop.fs.s3a.assumed.role.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
            .set("spark.sql.catalog.tabular", "org.apache.iceberg.spark.SparkCatalog")
            .set("spark.sql.catalog.tabular.catalog-impl", "org.apache.iceberg.rest.RESTCatalog")
            .set("spark.sql.catalog.tabular.uri", "https://api.dev.tabulardata.io/ws")
            .set("spark.sql.catalog.tabular.credential", System.getenv("TABULAR_EAST_CREDS"))
            .set("spark.sql.defaultCatalog", "tabular")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

    SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
    String metricDir = "s3://.../tsdb";
    String uidDir = "s3://.../tsdb-uid";
    String outputTable = "default.lumin_metrics";

    ConvertOptions convertOptions =
        ConvertOptions.builder()
            .metricDir(metricDir)
            .uidDir(uidDir)
            .outputTable(outputTable)
            .idSize(3)
            .fanout(false)
            .build();

    new Convert(spark, convertOptions).convert();
  }
}

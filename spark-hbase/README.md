# Spark OpenTSDB to Tabular converter

## Example Spark command

```bash
bin/spark-submit \
    --repositories https://tabular-repository-public.s3.amazonaws.com/releases \
    --packages io.tabular:tabular-client-runtime:0.29.3, \
        org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:0.14.1, \
        org.apache.hadoop:hadoop-aws:3.3.2, \
        org.apache.hbase:hbase-mapreduce:2.4.10 \
    --conf spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain \
    --conf spark.sql.catalog.tabular=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.tabular.catalog-impl=org.apache.iceberg.rest.RESTCatalog \
    --conf spark.sql.catalog.tabular.uri=https://api.dev.tabulardata.io/ws \
    --conf spark.sql.catalog.tabular.credential=$TABULAR_CREDS \
    --conf spark.sql.defaultCatalog=tabular \
    spark-hbase.jar \
      --metrics-dir s3://tabular-lumin/data/tsdb \
      --metrics-table temp.lumin_metrics \
      --uids-dir s3://tabular-lumin/data/tsdb-uids \
      --uids-table temp.lumin_uids
```

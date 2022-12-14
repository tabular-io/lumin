# Spark OpenTSDB-to-Iceberg converter

## Running in Spark

Build the shadow jar:
```bash
./gradlew clean build
```

Run using the `build/libs/convert-tsdb-all.jar` jar file:
```bash
bin/spark-submit \
    --repositories https://tabular-repository-public.s3.amazonaws.com/releases \
    --packages io.tabular:tabular-client-runtime:0.41.3,org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.0.0,org.apache.hadoop:hadoop-aws:3.3.2 \
    --driver-cores 8 \
    --driver-memory 16g \
    --executor-cores 4 \
    --executor-memory 24g \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider \
    --conf spark.hadoop.fs.s3a.assumed.role.arn=$TSDB_ARN \
    --conf spark.hadoop.fs.s3a.assumed.role.credentials.provider=com.amazonaws.auth.InstanceProfileCredentialsProvider \
    --conf spark.sql.catalog.tabular=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.tabular.catalog-impl=org.apache.iceberg.rest.RESTCatalog \
    --conf spark.sql.catalog.tabular.uri=https://api.dev.tabulardata.io/ws \
    --conf spark.sql.catalog.tabular.credential=$TABULAR_CREDS \
    --conf spark.sql.defaultCatalog=tabular \
    convert-tsdb-all.jar \
      --metric-dir s3://.../tsdb \
      --uid-dir s3://.../tsdb-uid \
      --output-table default.lumin_metrics \
      --size-of-id 3
```

Set `--size-of-id 3` for the legacy data format with 3 byte IDs. Newer format IDs are 4 bytes
and that is the default if not set.

Use the `--fanout` argument to enable fanout write instead of sorting. This will
eliminate the sort but will also use more memory, especially if an HFile contains
data for many hours. Also, fanout could create more files if data for the same
hour is spread across multiple HFiles.

Other options include `--limit-input-gb` and `--dry-run`, which can be used when testing.

Adjust memory, cores, and other Spark settings as needed.

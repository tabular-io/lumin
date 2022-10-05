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
    --packages io.tabular:tabular-client-runtime:0.29.3,org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:0.14.1,org.apache.hadoop:hadoop-aws:3.3.2 \
    --conf spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain \
    --conf spark.sql.catalog.tabular=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.tabular.catalog-impl=org.apache.iceberg.rest.RESTCatalog \
    --conf spark.sql.catalog.tabular.uri=https://api.dev.tabulardata.io/ws \
    --conf spark.sql.catalog.tabular.credential=$TABULAR_CREDS \
    --conf spark.sql.defaultCatalog=tabular \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    convert-tsdb-all.jar \
      --metric-dir s3://tabular-lumin/data/tsdb \
      --uid-dir s3://tabular-lumin/data/tsdb-uid \
      --output-table default.lumin_metrics
```

Adjust memory, cores, and other Spark settings as needed.

package lumin;

import java.io.Serializable;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.mapreduce.HFileInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;

public class Convert implements Serializable {

  private SparkSession spark;
  private String dataDir;
  private String dataTable;
  private String uidDir;
  private String uidTable;

  public Convert(
      SparkSession spark, String dataDir, String dataTable, String uidDir, String uidTable) {
    this.spark = spark;
    this.dataDir = dataDir;
    this.dataTable = dataTable;
    this.uidDir = uidDir;
    this.uidTable = uidTable;
  }

  public void convert() {
    writeTsdb();
    // TODO: writeTsdbUid();
  }

  private void writeTsdb() {
    StructType schema =
        StructType.fromDDL(
            "salt BINARY, metric_id BINARY, ts BINARY, tags MAP<BINARY, BINARY>, qualifier BINARY, value BINARY");

    Dataset<Row> df =
        loadHFiles(
            spark,
            dataDir,
            schema,
            cell -> {
              MetricRow m = new MetricRow(cell);
              return RowFactory.create(
                  m.salt, m.muid, m.ts, JavaConverters.mapAsScalaMap(m.tags), m.qualifier, m.value);
            });

    df.writeTo(dataTable).createOrReplace();
  }

  private void writeTsdbUid() {
    StructType schema = StructType.fromDDL("uid BINARY, qualifier BINARY, value STRING");

    Dataset<Row> df =
        loadHFiles(
            spark,
            uidDir,
            schema,
            cell -> {
              UIDRow m = new UIDRow(cell);
              return RowFactory.create(m.uid, m.qualifier, m.name);
            });

    df.writeTo(uidTable).createOrReplace();
  }

  private Dataset<Row> loadHFiles(
      SparkSession spark, String sourceDir, StructType schema, Function<Cell, Row> fn) {
    SparkContext ctx = spark.sparkContext();
    RDD<Row> rdd =
        ctx.newAPIHadoopFile(
                sourceDir,
                HFileInputFormat.class,
                NullWritable.class,
                Cell.class,
                ctx.hadoopConfiguration())
            .toJavaRDD()
            .map(tuple -> fn.call(tuple._2))
            // TODO: filter nulls/empty
            .rdd();
    return spark.createDataset(rdd, RowEncoder.apply(schema));
  }
}

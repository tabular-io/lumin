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
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.StructType;

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
    Dataset<Row> df =
        loadHFiles(spark, dataDir, MetricRow.SCHMEA, cell -> new MetricRow(cell).toRow());
    df.writeTo(dataTable).createOrReplace();
  }

  private void writeTsdbUid() {
    Dataset<Row> df = loadHFiles(spark, uidDir, UIDRow.SCHEMA, cell -> new UIDRow(cell).toRow());
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

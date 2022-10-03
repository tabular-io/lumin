package lumin;

import com.google.common.collect.Maps;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.mapreduce.HFileInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import scala.Tuple2;

public class Convert implements Serializable {

  private SparkSession spark;
  private String metricDir;
  private String metricTable;
  private String uidDir;
  private String uidTable;

  private Broadcast<List<UID>> uidBroadcast;

  public Convert(
      SparkSession spark, String metricDir, String metricTable, String uidDir, String uidTable) {
    this.spark = spark;
    this.metricDir = metricDir;
    this.metricTable = metricTable;
    this.uidDir = uidDir;
    this.uidTable = uidTable;
  }

  public void convert() {
    JavaRDD<UID> uidRdd = mapHFiles(uidDir, UID::fromCell);
    writeUidTable(uidRdd);

    List<UID> uidList = uidRdd.collect();
    uidBroadcast = new JavaSparkContext(spark.sparkContext()).broadcast(uidList);

    JavaRDD<Metric> metricRdd = flatMapHFiles(metricDir, new MetricMapFunction(uidBroadcast));
    writeTsdb(metricRdd);
  }

  private void writeUidTable(JavaRDD<UID> uidRdd) {
    spark
        .createDataset(uidRdd.map(UID::toRow).rdd(), RowEncoder.apply(UID.SCHEMA))
        .writeTo(uidTable)
        .createOrReplace();
  }

  private void writeTsdb(JavaRDD<Metric> metricRdd) {
    spark
        .createDataset(metricRdd.map(Metric::toRow).rdd(), RowEncoder.apply(Metric.SCHEMA))
        .limit(1000)
        .writeTo(metricTable)
        .createOrReplace();
  }

  private <T> JavaRDD<T> mapHFiles(String sourceDir, MapFunction<Cell, T> fn) {
    return createRDD(sourceDir).map(tuple -> fn.call(tuple._2)).filter(Objects::nonNull);
  }

  private <T> JavaRDD<T> flatMapHFiles(String sourceDir, FlatMapFunction<Cell, T> fn) {
    return createRDD(sourceDir).flatMap(tuple -> fn.call(tuple._2)).filter(Objects::nonNull);
  }

  private JavaRDD<Tuple2<NullWritable, Cell>> createRDD(String sourceDir) {
    SparkContext ctx = spark.sparkContext();
    return ctx.newAPIHadoopFile(
            sourceDir,
            HFileInputFormat.class,
            NullWritable.class,
            Cell.class,
            ctx.hadoopConfiguration())
        .toJavaRDD();
  }

  static class MetricMapFunction implements FlatMapFunction<Cell, Metric> {
    private final Broadcast<List<UID>> uidBroadcast;
    private transient Map<ByteBuffer, String> metricMap;
    private transient Map<ByteBuffer, String> tagKeyMap;
    private transient Map<ByteBuffer, String> tagValueMap;

    MetricMapFunction(Broadcast<List<UID>> uidBroadcast) {
      this.uidBroadcast = uidBroadcast;
    }

    @Override
    public Iterator<Metric> call(Cell cell) {
      if (metricMap == null) {
        loadMaps(uidBroadcast.value());
      }
      return Metric.fromCell(cell, metricMap, tagKeyMap, tagValueMap).iterator();
    }

    private void loadMaps(List<UID> uidList) {
      metricMap = Maps.newHashMap();
      tagKeyMap = Maps.newHashMap();
      tagValueMap = Maps.newHashMap();
      for (UID uid : uidList) {
        switch (uid.qualifier) {
          case "metrics":
            metricMap.put(ByteBuffer.wrap(uid.uid), uid.name);
            break;
          case "tagk":
            tagKeyMap.put(ByteBuffer.wrap(uid.uid), uid.name);
            break;
          case "tagv":
            tagValueMap.put(ByteBuffer.wrap(uid.uid), uid.name);
            break;
        }
      }
    }
  }
}

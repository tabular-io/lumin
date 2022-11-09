package io.tabular.tsdb.convert;

import com.google.common.collect.Lists;
import io.tabular.tsdb.convert.model.CellData;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled("integration test")
public class MiscTest {

  private static final int SALT_SIZE = 1;
  private static final int TS_SIZE = 4;
  private static final int ID_SIZE = 3;

  @Test
  public void analyze() {
    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("tsdb-analyze");
    SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
    JavaRDD<Timestamp> rdd =
        new JavaSparkContext(spark.sparkContext())
            .parallelize(Arrays.asList("file:/Users/bryan/Downloads/lumin-sample.hfile"))
            .flatMap(
                new HFileToCellData(new ConfigHolder(spark.sparkContext().hadoopConfiguration())))
            .flatMap(MiscTest::extractTimestamp);

    Timestamp minTs = rdd.min(new TsCompare());
    Timestamp maxTs = rdd.max(new TsCompare());

    System.out.println("Min time: " + minTs);
    System.out.println("Max time: " + maxTs);
  }

  public static Iterator<Timestamp> extractTimestamp(CellData cellData) {
    List<Timestamp> result = Lists.newArrayList();

    byte[] qualifierBytes = cellData.getQualifier();

    if (qualifierBytes.length == 3 || qualifierBytes.length == 5) {
      // this is an annotation or other non-datapoint object, filter these out
      return result.iterator();
    }

    int prefixSize = SALT_SIZE + TS_SIZE + ID_SIZE;
    byte[] rowKey = cellData.getRowKey();

    // Validate size for prefix and tag k/v ids
    if (rowKey.length < prefixSize || (rowKey.length - prefixSize) % (ID_SIZE * 2) != 0) {
      throw new RuntimeException("Unsupported row key format");
    }

    byte[] tsBytes = new byte[TS_SIZE];
    System.arraycopy(rowKey, SALT_SIZE + ID_SIZE, tsBytes, 0, TS_SIZE);
    long baseMillis = ByteBuffer.wrap(tsBytes).getInt() * 1000L;

    int numQualifiers = qualifierBytes.length / 2;

    for (int qualifierOffset = 0; qualifierOffset < numQualifiers; qualifierOffset += 2) {
      short qualifier = ByteBuffer.wrap(qualifierBytes, qualifierOffset, 2).getShort();
      int offsetSec = qualifier >> 4;
      Timestamp ts = new Timestamp(baseMillis + (1000L * offsetSec));

      result.add(ts);
    }
    return result.iterator();
  }

  public static class TsCompare implements Comparator<Timestamp>, Serializable {

    @Override
    public int compare(Timestamp o1, Timestamp o2) {
      return o1.compareTo(o2);
    }
  }
}

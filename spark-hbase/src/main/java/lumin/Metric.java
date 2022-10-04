package lumin;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;

public class Metric implements Serializable {

  public static final StructType SCHEMA =
      StructType.fromDDL("metric STRING, ts TIMESTAMP, value DOUBLE, tags MAP<STRING, STRING>");

  private static final int SALT_BYTES = 1;
  private static final int UID_BYTES = 4;
  private static final int TS_BYTES = 4;
  private static final int PREFIX_BYTES = SALT_BYTES + UID_BYTES + TS_BYTES;
  private static final int TAG_BYTES = 4;

  String metricName;
  Map<String, String> tags;
  double value;
  Timestamp ts;

  public Metric(String metricName, Map<String, String> tags, double value, Timestamp ts) {
    this.metricName = metricName;
    this.tags = tags;
    this.value = value;
    this.ts = ts;
  }

  public static List<Metric> fromCellData(
      CellData cellData,
      Map<ByteBuffer, String> metricMap,
      Map<ByteBuffer, String> tagKeyMap,
      Map<ByteBuffer, String> tagValueMap) {

    byte[] rowKey = cellData.rowKey;

    // Validate size for prefix and tag k/v ids
    if (rowKey.length < PREFIX_BYTES || (rowKey.length - PREFIX_BYTES) % (TAG_BYTES * 2) != 0) {
      throw new RuntimeException("Unsupported row key format");
    }

    byte[] metricId = new byte[UID_BYTES];
    System.arraycopy(rowKey, SALT_BYTES, metricId, 0, UID_BYTES);
    String metricName = metricMap.get(ByteBuffer.wrap(metricId));
    if (metricName == null) {
      throw new RuntimeException("Unable to map metric ID to name");
    }

    byte[] tsBytes = new byte[TS_BYTES];
    System.arraycopy(rowKey, SALT_BYTES + UID_BYTES, tsBytes, 0, TS_BYTES);
    long baseMillis = ByteBuffer.wrap(tsBytes).getInt() * 1000L;

    int tagCount = (rowKey.length - PREFIX_BYTES) / (TAG_BYTES * 2);
    int pos = PREFIX_BYTES;

    Map<String, String> tags = Maps.newHashMap();
    for (int i = 0; i < tagCount; i++) {
      byte[] tagk = new byte[TAG_BYTES];
      byte[] tagv = new byte[TAG_BYTES];

      System.arraycopy(rowKey, pos, tagk, 0, TAG_BYTES);
      pos += TAG_BYTES;
      System.arraycopy(rowKey, pos, tagv, 0, TAG_BYTES);
      pos += TAG_BYTES;

      String tagkStr = tagKeyMap.get(ByteBuffer.wrap(tagk));
      if (tagkStr == null) {
        throw new RuntimeException("Unable to map tag key to name");
      }

      String tagvStr = tagValueMap.get(ByteBuffer.wrap(tagv));
      if (tagvStr == null) {
        throw new RuntimeException("Unable to map tag value to name");
      }

      tags.put(tagkStr, tagvStr);
    }

    byte[] qualifierBytes = cellData.qualifier;
    ;
    if (qualifierBytes.length % 2 != 0) {
      throw new RuntimeException("Unexpected qualifier, odd number of bytes");
    }
    byte[] valueBytes = cellData.value;

    List<Metric> result = Lists.newArrayList();

    int numQualifiers = qualifierBytes.length / 2;
    int valueOffset = 0;

    for (int qualifierOffset = 0; qualifierOffset < numQualifiers; qualifierOffset += 2) {
      short qualifier = ByteBuffer.wrap(qualifierBytes, qualifierOffset, 2).getShort();
      int offsetSec = qualifier >> 4;
      Timestamp ts = new Timestamp(baseMillis + (1000L * offsetSec));

      boolean isInt = (qualifier & 0b1000) == 0;
      int valueLen = (qualifier & 0b111) + 1;
      double value = parseValue(isInt, valueBytes, valueOffset, valueLen);
      valueOffset += valueLen;

      result.add(new Metric(metricName, tags, value, ts));
    }
    return result;
  }

  private static double parseValue(
      boolean isInt, byte[] valueBytes, int valueOffset, int valueLen) {
    double value;
    switch (valueLen) {
      case 1:
        if (!isInt) {
          throw new RuntimeException("Invalid float length: " + valueLen);
        }
        value = valueBytes[valueOffset];
        break;

      case 2:
        if (!isInt) {
          throw new RuntimeException("Invalid float length: " + valueLen);
        }
        value = ByteBuffer.wrap(valueBytes, valueOffset, 2).getShort();
        break;

      case 4:
        if (isInt) {
          value = ByteBuffer.wrap(valueBytes, valueOffset, 4).getInt();
        } else {
          value = ByteBuffer.wrap(valueBytes, valueOffset, 4).getFloat();
        }
        break;

      case 8:
        if (isInt) {
          value = ByteBuffer.wrap(valueBytes, valueOffset, 8).getLong();
        } else {
          value = ByteBuffer.wrap(valueBytes, valueOffset, 8).getDouble();
        }
        break;

      default:
        throw new RuntimeException("Invalid value length: " + valueLen);
    }

    return value;
  }

  public Row toRow() {
    return RowFactory.create(metricName, ts, value, JavaConverters.mapAsScalaMap(tags));
  }
}

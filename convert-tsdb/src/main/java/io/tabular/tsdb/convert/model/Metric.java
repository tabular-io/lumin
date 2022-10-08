package io.tabular.tsdb.convert.model;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;

@AllArgsConstructor
public class Metric implements Serializable {

  public static final StructType SCHEMA =
      StructType.fromDDL("metric STRING, ts TIMESTAMP, value DOUBLE, tags MAP<STRING, STRING>");

  private static final int SALT_SIZE = 1;
  private static final int TS_SIZE = 4;

  private String metricName;
  private Map<String, String> tags;
  private double value;
  private Timestamp ts;

  public static List<Metric> fromCellData(
      CellData cellData,
      Map<ByteBuffer, String> metricMap,
      Map<ByteBuffer, String> tagKeyMap,
      Map<ByteBuffer, String> tagValueMap,
      int idSize) {

    List<Metric> result = Lists.newArrayList();

    byte[] qualifierBytes = cellData.getQualifier();

    if (qualifierBytes.length == 3 || qualifierBytes.length == 5) {
      // this is an annotation or other non-datapoint object, filter these out
      return result;
    }

    byte[] rowKey = cellData.getRowKey();
    byte[] valueBytes = cellData.getValue();

    int prefixSize = SALT_SIZE + TS_SIZE + idSize;

    // Validate size for prefix and tag k/v ids
    if (rowKey.length < prefixSize || (rowKey.length - prefixSize) % (idSize * 2) != 0) {
      throw new RuntimeException("Unsupported row key format");
    }

    byte[] metricId = new byte[idSize];
    System.arraycopy(rowKey, SALT_SIZE, metricId, 0, idSize);
    String metricName = metricMap.get(ByteBuffer.wrap(metricId));
    if (metricName == null) {
      throw new RuntimeException("Unable to map metric ID to name");
    }

    byte[] tsBytes = new byte[TS_SIZE];
    System.arraycopy(rowKey, SALT_SIZE + idSize, tsBytes, 0, TS_SIZE);
    long baseMillis = ByteBuffer.wrap(tsBytes).getInt() * 1000L;

    int tagCount = (rowKey.length - prefixSize) / (idSize * 2);
    int pos = prefixSize;

    Map<String, String> tags = Maps.newHashMap();
    for (int i = 0; i < tagCount; i++) {
      byte[] tagk = new byte[idSize];
      byte[] tagv = new byte[idSize];

      System.arraycopy(rowKey, pos, tagk, 0, idSize);
      pos += idSize;
      System.arraycopy(rowKey, pos, tagv, 0, idSize);
      pos += idSize;

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

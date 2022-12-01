package io.tabular.tsdb.convert.model;

import static io.tabular.tsdb.convert.Utilities.bytesToInt;
import static io.tabular.tsdb.convert.Utilities.bytesToLong;
import static io.tabular.tsdb.convert.Utilities.bytesToShort;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.Serializable;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructType;
import org.sparkproject.guava.collect.Iterators;
import scala.collection.JavaConverters;

@AllArgsConstructor
@Getter
public class Metric implements Serializable {

  public static final StructType SCHEMA =
      StructType.fromDDL(
          "metric STRING,"
              + "time TIMESTAMP,"
              + "value DOUBLE,"
              + "lsp_id INT,"
              + "circuit_id INT,"
              + "power_sign STRING,"
              + "tags MAP<STRING, STRING>,"
              + "version BIGINT");

  private static final int SALT_SIZE = 1;
  private static final int TS_SIZE = 4;
  private static final long HOUR_MILLIS = Duration.ofHours(1).toMillis();

  private static final String LSP_ID_TAG = "bot";
  private static final String CIRCUIT_ID_TAG = "circuit";
  private static final String POWER_SIGN_TAG = "sign";

  private static final byte[] APPEND_QUALIFIER = new byte[] {0x05, 0x00, 0x00};

  private final String metricName;
  private final Timestamp ts;
  private final double value;
  private final Integer lspId;
  private final Integer circuitId;
  private final String powerSign;
  private final Map<String, String> tags;
  private final byte[] salt;
  private final long version;
  private final String file;

  public static Iterator<Metric> fromCellData(
      CellData cellData,
      Map<Integer, String> metricMap,
      Map<Integer, String> tagKeyMap,
      Map<Integer, String> tagValueMap,
      int idSize) {

    if (cellData.getFamily().length != 1 || cellData.getFamily()[0] != 't') {
      return Iterators.emptyIterator();
    }

    byte[] qualifierBytes = cellData.getQualifier();

    boolean appendDataPoint = false;
    if ((qualifierBytes.length == 3 || qualifierBytes.length == 5) && qualifierBytes[0] == 0x01) {
      // this is an annotation or other non-datapoint object, filter these out
      return Iterators.emptyIterator();
    } else if (Arrays.equals(qualifierBytes, APPEND_QUALIFIER)) {
      appendDataPoint = true;
    } else if (qualifierBytes.length % 2 != 0) {
      throw new RuntimeException("Invalid qualifier");
    }

    byte[] rowKey = cellData.getRowKey();

    byte[] salt = new byte[SALT_SIZE];
    System.arraycopy(rowKey, 0, salt, 0, SALT_SIZE);

    int prefixSize = SALT_SIZE + TS_SIZE + idSize;

    // Validate size for prefix and tag k/v ids
    if (rowKey.length < prefixSize || (rowKey.length - prefixSize) % (idSize * 2) != 0) {
      throw new RuntimeException("Unsupported row key format");
    }

    byte[] metricId = new byte[idSize];
    System.arraycopy(rowKey, SALT_SIZE, metricId, 0, idSize);
    String metricName = metricMap.get(bytesToInt(metricId));
    if (metricName == null) {
      throw new RuntimeException("Unable to map metric ID to name");
    }

    byte[] tsBytes = new byte[TS_SIZE];
    System.arraycopy(rowKey, SALT_SIZE + idSize, tsBytes, 0, TS_SIZE);
    long baseMillis = bytesToLong(tsBytes, 0, 4) * 1000L;

    int tagCount = (rowKey.length - prefixSize) / (idSize * 2);
    int pos = prefixSize;

    Map<String, String> tags = Maps.newHashMapWithExpectedSize(tagCount);
    Integer lspId = null, circuitId = null;
    String powerSign = null;
    for (int i = 0; i < tagCount; i++) {
      byte[] tagk = new byte[idSize];
      byte[] tagv = new byte[idSize];

      System.arraycopy(rowKey, pos, tagk, 0, idSize);
      pos += idSize;
      System.arraycopy(rowKey, pos, tagv, 0, idSize);
      pos += idSize;

      String tagkStr = tagKeyMap.get(bytesToInt(tagk));
      if (tagkStr == null) {
        throw new RuntimeException("Unable to map tag key to name");
      }

      String tagvStr = tagValueMap.get(bytesToInt(tagv));
      if (tagvStr == null) {
        throw new RuntimeException("Unable to map tag value to name");
      }

      switch (tagkStr) {
        case LSP_ID_TAG:
          try {
            lspId = Integer.parseInt(tagvStr);
          } catch (NumberFormatException x) {
            System.out.println("***** WARNING: Unable to set lsp ID: " + tagvStr);
          }
          break;
        case CIRCUIT_ID_TAG:
          try {
            circuitId = Integer.parseInt(tagvStr);
          } catch (NumberFormatException x) {
            System.out.println("***** WARNING: Unable to set circuit ID: " + tagvStr);
          }
          break;
        case POWER_SIGN_TAG:
          powerSign = tagvStr;
          break;
        default:
          tags.put(tagkStr, tagvStr);
      }
    }

    List<Metric> result = Lists.newLinkedList();
    byte[] valueBytes = cellData.getValue();
    int valueOffset = 0;

    if (!appendDataPoint) {
      for (int qualifierOffset = 0;
          qualifierOffset < qualifierBytes.length - 1;
          qualifierOffset += 2) {
        int qualifier = bytesToInt(qualifierBytes, qualifierOffset, 2);
        long offsetMillis = (qualifier >> 4) * 1000L;

        if (offsetMillis < 0 || offsetMillis > HOUR_MILLIS) {
          throw new RuntimeException("Invalid offset millis: " + offsetMillis);
        }

        Timestamp ts = new Timestamp(baseMillis + offsetMillis);

        boolean isInt = (qualifier & 0b1000) == 0;
        int valueLen = (qualifier & 0b111) + 1;
        double value = parseValue(isInt, valueBytes, valueOffset, valueLen);
        valueOffset += valueLen;

        result.add(
            new Metric(
                metricName,
                ts,
                value,
                lspId,
                circuitId,
                powerSign,
                tags,
                salt,
                cellData.getVersion(),
                cellData.getFile()));
      }

      if (result.size() == 1 && valueOffset < valueBytes.length) {
        throw new RuntimeException("Value bytes not fully utilized");
      } else if (result.size() > 1
          && (valueOffset != valueBytes.length - 1 || valueBytes[valueOffset] != 0)) {
        throw new RuntimeException("Value bytes not fully utilized");
      }
    } else {
      while (valueOffset < valueBytes.length) {
        int qualifier = bytesToInt(valueBytes, valueOffset, 2);
        valueOffset += 2;
        int offsetSec = qualifier >> 4;
        Timestamp ts = new Timestamp(baseMillis + (1000L * offsetSec));

        boolean isInt = (qualifier & 0b1000) == 0;
        int valueLen = (qualifier & 0b111) + 1;
        double value = parseValue(isInt, valueBytes, valueOffset, valueLen);
        valueOffset += valueLen;

        result.add(
            new Metric(
                metricName,
                ts,
                value,
                lspId,
                circuitId,
                powerSign,
                tags,
                salt,
                cellData.getVersion(),
                cellData.getFile()));
      }
    }

    return result.iterator();
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
        value = bytesToShort(valueBytes, valueOffset, valueLen);
        break;

      case 4:
        if (isInt) {
          value = bytesToInt(valueBytes, valueOffset, valueLen);
        } else {
          value = Float.intBitsToFloat(bytesToInt(valueBytes, valueOffset, valueLen));
        }
        break;

      case 8:
        if (isInt) {
          value = bytesToLong(valueBytes, valueOffset, valueLen);
        } else {
          value = Double.longBitsToDouble(bytesToLong(valueBytes, valueOffset, valueLen));
        }
        break;

      default:
        throw new RuntimeException("Invalid value length: " + valueLen);
    }

    return value;
  }

  public Row toRow() {
    return RowFactory.create(
        metricName,
        ts,
        value,
        lspId,
        circuitId,
        powerSign,
        JavaConverters.mapAsScalaMap(tags),
        version);
  }
}

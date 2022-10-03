package lumin;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructType;

public class UID implements Serializable {
  public static final StructType SCHEMA =
      StructType.fromDDL("uid BINARY, qualifier STRING, value STRING");

  byte[] uid;
  String qualifier;
  String name;

  public UID(byte[] uid, String qualifier, String name) {
    this.uid = uid;
    this.qualifier = qualifier;
    this.name = name;
  }

  public static UID fromCell(Cell cell) {
    String family = new String(CellUtil.cloneFamily(cell), StandardCharsets.UTF_8);
    if (!"name".equals(family)) {
      return null;
    }

    byte[] uid = CellUtil.cloneRow(cell);
    String qualifier = new String(CellUtil.cloneQualifier(cell), StandardCharsets.UTF_8);
    String name = new String(CellUtil.cloneValue(cell), StandardCharsets.UTF_8);

    return new UID(uid, qualifier, name);
  }

  public Row toRow() {
    return RowFactory.create(uid, qualifier, name);
  }
}

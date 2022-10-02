package lumin;

import java.nio.charset.StandardCharsets;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructType;

public class UIDRow {
  public static final StructType SCHEMA =
      StructType.fromDDL("uid BINARY, qualifier BINARY, value STRING");

  private byte[] uid;
  private byte[] qualifier;
  private String name;

  public UIDRow(Cell cell) {
    String family = new String(CellUtil.cloneFamily(cell), StandardCharsets.UTF_8);
    if (!"name".equals(family)) {
      return;
    }

    this.uid = CellUtil.cloneRow(cell);
    this.qualifier = CellUtil.cloneQualifier(cell);
    this.name = new String(CellUtil.cloneValue(cell), StandardCharsets.UTF_8);
  }

  public Row toRow() {
    return RowFactory.create(uid, qualifier, name);
  }
}

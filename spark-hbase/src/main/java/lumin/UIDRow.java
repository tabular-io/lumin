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

  public static Row convertCellToRow(Cell cell) {
    String family = new String(CellUtil.cloneFamily(cell), StandardCharsets.UTF_8);
    if (!"name".equals(family)) {
      return null;
    }

    byte[] uid = CellUtil.cloneRow(cell);
    byte[] qualifier = CellUtil.cloneQualifier(cell);
    String name = new String(CellUtil.cloneValue(cell), StandardCharsets.UTF_8);

    return RowFactory.create(uid, qualifier, name);
  }
}

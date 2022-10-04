package lumin;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;

public class UID implements Serializable {
  byte[] uid;
  String qualifier;
  String name;

  public UID(byte[] uid, String qualifier, String name) {
    this.uid = uid;
    this.qualifier = qualifier;
    this.name = name;
  }

  public static UID fromCellData(CellData cellData) {
    String family = new String(cellData.family, StandardCharsets.UTF_8);
    if (!"name".equals(family)) {
      return null;
    }

    byte[] uid = cellData.rowKey;
    String qualifier = new String(cellData.qualifier, StandardCharsets.UTF_8);
    String name = new String(cellData.value, StandardCharsets.UTF_8);

    return new UID(uid, qualifier, name);
  }
}

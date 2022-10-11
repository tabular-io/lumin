package io.tabular.tsdb.convert.model;

import static io.tabular.tsdb.convert.Utilities.bytesToInt;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class UID implements Serializable {
  private final int uid;
  private final String qualifier;
  private final String name;

  public static UID fromCellData(CellData cellData) {
    String family = new String(cellData.getFamily(), StandardCharsets.UTF_8);
    if (!"name".equals(family)) {
      return null;
    }

    if (cellData.getRowKey().length > 4) {
      throw new RuntimeException(
          "ID size must be 4 bytes or less, but was: " + cellData.getRowKey().length);
    }

    int uid = bytesToInt(cellData.getRowKey());
    String qualifier = new String(cellData.getQualifier(), StandardCharsets.UTF_8);
    String name = new String(cellData.getValue(), StandardCharsets.UTF_8);

    return new UID(uid, qualifier, name);
  }
}

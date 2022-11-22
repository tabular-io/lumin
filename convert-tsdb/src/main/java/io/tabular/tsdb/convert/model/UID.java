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
    if (!"id".equals(family)) {
      return null;
    }

    if (cellData.getRowKey().length == 1 && cellData.getRowKey()[0] == 0) {
      // this key keeps track of the max ID per qualifier
      return null;
    }

    if (cellData.getValue().length > 4) {
      throw new RuntimeException(
          "ID size must be 4 bytes or less, but was: " + cellData.getValue().length);
    }

    int uid = bytesToInt(cellData.getValue());
    String qualifier = new String(cellData.getQualifier(), StandardCharsets.UTF_8);
    String name = new String(cellData.getRowKey(), StandardCharsets.UTF_8);

    return new UID(uid, qualifier, name);
  }
}

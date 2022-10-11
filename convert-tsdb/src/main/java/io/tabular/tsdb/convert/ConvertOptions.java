package io.tabular.tsdb.convert;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;

@Builder
@Getter
public class ConvertOptions {
  @NonNull private String metricDir;
  @NonNull private String uidDir;
  @NonNull private String outputTable;
  private int idSize;
  private boolean fanout;
  private int limitGb;
  private boolean dryRun;
}

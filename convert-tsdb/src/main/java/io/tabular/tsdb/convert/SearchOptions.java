package io.tabular.tsdb.convert;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;

@Builder
@Getter
public class SearchOptions {
  @NonNull private String metricDir;
  @NonNull private String uidDir;
  private int idSize;
}

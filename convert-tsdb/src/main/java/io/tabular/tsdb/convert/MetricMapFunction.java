package io.tabular.tsdb.convert;

import com.google.common.collect.Maps;
import io.tabular.tsdb.convert.model.CellData;
import io.tabular.tsdb.convert.model.Metric;
import io.tabular.tsdb.convert.model.UID;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

class MetricMapFunction implements FlatMapFunction<CellData, Metric> {

  private final Broadcast<List<UID>> uidBroadcast;
  private final int idSize;
  private transient Map<Integer, String> metricMap;
  private transient Map<Integer, String> tagKeyMap;
  private transient Map<Integer, String> tagValueMap;

  MetricMapFunction(Broadcast<List<UID>> uidBroadcast, int idSize) {
    this.uidBroadcast = uidBroadcast;
    this.idSize = idSize;
  }

  @Override
  public Iterator<Metric> call(CellData cellData) {
    if (metricMap == null) {
      loadMaps(uidBroadcast.value());
    }
    return Metric.fromCellData(cellData, metricMap, tagKeyMap, tagValueMap, idSize);
  }

  private void loadMaps(List<UID> uidList) {
    metricMap = Maps.newHashMap();
    tagKeyMap = Maps.newHashMap();
    tagValueMap = Maps.newHashMap();
    for (UID uid : uidList) {
      switch (uid.getQualifier()) {
        case "metrics":
          metricMap.put(uid.getUid(), uid.getName());
          break;
        case "tagk":
          tagKeyMap.put(uid.getUid(), uid.getName());
          break;
        case "tagv":
          tagValueMap.put(uid.getUid(), uid.getName());
          break;
      }
    }
  }
}

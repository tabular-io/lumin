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

    try {
      return Metric.fromCellData(cellData, metricMap, tagKeyMap, tagValueMap, idSize);
    } catch (Exception x) {
      throw new RuntimeException("Metric decoding error in file: " + cellData.getFile(), x);
    }
  }

  private void loadMaps(List<UID> uidList) {
    metricMap = Maps.newHashMap();
    tagKeyMap = Maps.newHashMap();
    tagValueMap = Maps.newHashMap();

    for (UID uid : uidList) {
      switch (uid.getQualifier()) {
        case "metrics":
          if (metricMap.containsKey(uid.getUid())) {
            throw new RuntimeException("Duplicate UID");
          }
          metricMap.put(uid.getUid(), uid.getName());
          break;
        case "tagk":
          if (tagKeyMap.containsKey(uid.getUid())) {
            throw new RuntimeException("Duplicate UID");
          }
          tagKeyMap.put(uid.getUid(), uid.getName());
          break;
        case "tagv":
          if (tagValueMap.containsKey(uid.getUid())) {
            throw new RuntimeException("Duplicate UID");
          }
          tagValueMap.put(uid.getUid(), uid.getName());
          break;
        default:
          throw new RuntimeException("Unknown UID qualifier: " + uid.getQualifier());
      }
    }
  }
}

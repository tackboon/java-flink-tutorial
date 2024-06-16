package flink.source.customSplitReader.split;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class IntRangeSplitState implements Serializable {
  private final Map<String, IntRangeSplit> splits;

  public IntRangeSplitState(IntRangeSplit split) {
    Map<String, IntRangeSplit> splitsMap = new HashMap<>();
    splitsMap.put(split.splitId(), split);
    this.splits = splitsMap;
  }

  public Map<String, IntRangeSplit> getSplits() {
    return splits;
  }
}

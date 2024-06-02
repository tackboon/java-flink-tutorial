package flink.app.customSource.split;

import org.apache.flink.api.connector.source.SourceEvent;

public class IntRangeCompletedSplit implements SourceEvent {
  private final String splitId;

  public IntRangeCompletedSplit(String splitId) {
    this.splitId = splitId;
  }

  public String getSplitId() {
    return splitId;
  }
}

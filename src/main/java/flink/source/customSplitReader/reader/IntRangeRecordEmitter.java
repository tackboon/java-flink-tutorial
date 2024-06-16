package flink.source.customSplitReader.reader;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

import flink.source.customSplitReader.split.IntRangeSplitState;

public class IntRangeRecordEmitter implements RecordEmitter<Integer, Integer, IntRangeSplitState> {
  @Override
  public void emitRecord(Integer in, SourceOutput<Integer> out, IntRangeSplitState splitState) throws Exception {
    out.collect(in);
  }
}

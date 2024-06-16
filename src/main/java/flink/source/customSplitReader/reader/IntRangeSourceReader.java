package flink.source.customSplitReader.reader;

import java.util.Map;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import flink.source.customSplitReader.split.IntRangeSplit;
import flink.source.customSplitReader.split.IntRangeSplitState;

public class IntRangeSourceReader
    extends SingleThreadMultiplexSourceReaderBase<Integer, Integer, IntRangeSplit, IntRangeSplitState> {
  private static final Logger log = LoggerFactory.getLogger(IntRangeSourceReader.class);

  public IntRangeSourceReader(
      SourceReaderContext readerContext,
      RecordEmitter<Integer, Integer, IntRangeSplitState> recordEmitter,
      Configuration configuration) {
    super(IntRangeSplitReader::new, recordEmitter, configuration, readerContext);
    log.info("========= initializing source reader");
  }

  @Override
  protected IntRangeSplitState initializedState(IntRangeSplit split) {
    log.info("========= initializing source reader state");
    return new IntRangeSplitState(split);
  }

  @Override
  protected IntRangeSplit toSplitType(String splitID, IntRangeSplitState state) {
    log.info("========= converting split state to split on source reader");
    return state.getSplits().get(splitID);
  }

  @Override
  protected void onSplitFinished(Map<String, IntRangeSplitState> finishedSplits) {
    log.info("========= finished splits");
  }
}

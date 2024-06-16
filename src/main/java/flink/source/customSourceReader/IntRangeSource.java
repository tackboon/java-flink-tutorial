package flink.source.customSourceReader;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import flink.source.customSourceReader.enumerator.IntRangeEnumeratorState;
import flink.source.customSourceReader.enumerator.IntRangeEnumeratorStateSerializer;
import flink.source.customSourceReader.enumerator.IntRangeEnumerator;
import flink.source.customSourceReader.reader.IntRangeReader;
import flink.source.customSourceReader.split.IntRangeSplit;
import flink.source.customSourceReader.split.IntRangeSplitSerializer;

public class IntRangeSource implements Source<Integer, IntRangeSplit, IntRangeEnumeratorState> {
  private static final Logger log = LoggerFactory.getLogger(IntRangeSource.class);

  @Override
  public Boundedness getBoundedness() {
    log.info("========= get boundedness");
    return Boundedness.CONTINUOUS_UNBOUNDED;
  }

  @Override
  public SimpleVersionedSerializer<IntRangeSplit> getSplitSerializer() {
    log.info("========= getting split serializer");
    return new IntRangeSplitSerializer();
  }

  @Override
  public SimpleVersionedSerializer<IntRangeEnumeratorState> getEnumeratorCheckpointSerializer() {
    log.info("========= getting enumerator checkpoint serializer");
    return new IntRangeEnumeratorStateSerializer();
  }

  @Override
  public SplitEnumerator<IntRangeSplit, IntRangeEnumeratorState> createEnumerator(
      SplitEnumeratorContext<IntRangeSplit> enumContext) throws Exception {
    log.info("========= creating enumerator");

    return new IntRangeEnumerator(enumContext);
  }

  @Override
  public SplitEnumerator<IntRangeSplit, IntRangeEnumeratorState> restoreEnumerator(
      SplitEnumeratorContext<IntRangeSplit> enumContext, IntRangeEnumeratorState checkpoint) throws Exception {
    log.info("========= restoring enumerator");
    return new IntRangeEnumerator(enumContext, checkpoint);
  }

  @Override
  public SourceReader<Integer, IntRangeSplit> createReader(SourceReaderContext readerContext) throws Exception {
    log.info("========= creating reader");
    return new IntRangeReader(readerContext);
  }
}

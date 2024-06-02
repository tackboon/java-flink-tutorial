package flink.app.customSource.source;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import flink.app.customSource.enumerator.EnumeratorState;
import flink.app.customSource.enumerator.EnumeratorStateSerializer;
import flink.app.customSource.enumerator.IntRangeEnumerator;
import flink.app.customSource.reader.IntRangeReader;
import flink.app.customSource.split.IntRangeSplit;
import flink.app.customSource.split.IntRangeSplitSerializer;

public class IntRangeSource implements Source<Integer, IntRangeSplit, EnumeratorState> {
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
  public SimpleVersionedSerializer<EnumeratorState> getEnumeratorCheckpointSerializer() {
    log.info("========= getting enumerator checkpoint serializer");

    return new EnumeratorStateSerializer();
  }

  @Override
  public SplitEnumerator<IntRangeSplit, EnumeratorState> createEnumerator(
      SplitEnumeratorContext<IntRangeSplit> enumContext) throws Exception {
    log.info("========= creating enumerator");

    return new IntRangeEnumerator(enumContext);
  }

  @Override
  public SourceReader<Integer, IntRangeSplit> createReader(SourceReaderContext readerContext) throws Exception {
    log.info("========= creating reader");

    return new IntRangeReader(readerContext);
  }

  @Override
  public SplitEnumerator<IntRangeSplit, EnumeratorState> restoreEnumerator(
      SplitEnumeratorContext<IntRangeSplit> enumContext, EnumeratorState checkpoint) throws Exception {
    log.info("========= restoring enumerator");

    return new IntRangeEnumerator(enumContext, checkpoint);
  }
}

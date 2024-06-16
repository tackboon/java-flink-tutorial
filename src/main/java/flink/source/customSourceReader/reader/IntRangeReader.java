package flink.source.customSourceReader.reader;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import flink.source.customSourceReader.split.IntRangeCompletedSplit;
import flink.source.customSourceReader.split.IntRangeSplit;

public class IntRangeReader implements SourceReader<Integer, IntRangeSplit> {
  private static final Logger log = LoggerFactory.getLogger(IntRangeReader.class);

  private IntRangeSplit currentSplit = null;
  private CompletableFuture<Void> availability = CompletableFuture.completedFuture(null);
  private final SourceReaderContext context;

  public IntRangeReader(SourceReaderContext context) {
    log.info("========= creating int range reader");

    this.context = context;
  }

  @Override
  public void start() {
    log.info("========= staring int range reader");
  }

  @Override
  public InputStatus pollNext(ReaderOutput<Integer> output) {
    if (currentSplit != null && currentSplit.getCurrentValue() < currentSplit.getUntil()) {
      log.info("========= polling from int range reader 1");

      if (currentSplit.getCurrentValue() == 13) {
        throw new RuntimeException("artificial error at value 13");
      }

      output.collect(currentSplit.getCurrentValue());
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      currentSplit.setCurrentValue(currentSplit.getCurrentValue() + 1);
      return InputStatus.MORE_AVAILABLE;
    } else {
      log.info("========= polling from int range reader 2");

      if (currentSplit != null && currentSplit.getCurrentValue() >= currentSplit.getUntil()) {
        context.sendSourceEventToCoordinator(new IntRangeCompletedSplit(currentSplit.splitId()));
      }

      if (availability.isDone()) {
        availability = new CompletableFuture<>();
        context.sendSplitRequest();
      }

      return InputStatus.NOTHING_AVAILABLE;
    }
  }

  @Override
  public List<IntRangeSplit> snapshotState(long checkpointId) {
    log.info("========= snapshotting state from int range reader, checkpoint_id: " + checkpointId);

    return List.of(currentSplit);
  }

  @Override
  public CompletableFuture<Void> isAvailable() {
    log.info("====== checking is int range reader available");

    return availability;
  }

  @Override
  public void addSplits(List<IntRangeSplit> splits) {
    if (splits.isEmpty()) {
      log.info("========= adding splits from int range reader");
    } else {
      log.info("========= adding splits from int range reader, current split: " + splits.get(0).toString());
    }

    // Only one split is assigned per task in the enumerator, so we only use the
    // first index.
    currentSplit = splits.get(0);
    // Data availability is over since we got a split.
    availability.complete(null);
  }

  @Override
  public void notifyNoMoreSplits() {
    // Not implemented since we expect our source to be boundless.
    log.info("========= notifing no more splits from int range reader");
  }

  @Override
  public void close() throws Exception {
    log.info("========= closing int range reader");
  }
}

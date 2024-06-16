package flink.source.customSourceReader.enumerator;

import java.util.List;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import flink.source.customSourceReader.split.IntRangeCompletedSplit;
import flink.source.customSourceReader.split.IntRangeSplit;

public class IntRangeEnumerator implements SplitEnumerator<IntRangeSplit, IntRangeEnumeratorState> {
  private static final Logger log = LoggerFactory.getLogger(IntRangeEnumerator.class);

  private final SplitEnumeratorContext<IntRangeSplit> context;
  private final IntRangeEnumeratorState state;

  public IntRangeEnumerator(SplitEnumeratorContext<IntRangeSplit> context) {
    log.info("========= creating enumerator");

    this.context = context;
    this.state = new IntRangeEnumeratorState(0);
  }

  public IntRangeEnumerator(SplitEnumeratorContext<IntRangeSplit> context, IntRangeEnumeratorState state) {
    log.info("========= creating enumerator with state, state: ", state);

    this.context = context;
    this.state = state;
  }

  @Override
  public void start() {
    log.info("========= starting enumerator");
  }

  @Override
  public void handleSplitRequest(int subtaskId, String requesterHostname) {
    if (!state.getDeadSplits().isEmpty()) {
      log.info(String.format(
          "========= handling split request from enumerator with dead splits, subtask_id: %d, requester_hostname: %s",
          subtaskId, requesterHostname));

      IntRangeSplit split = state.getDeadSplits().remove(0);
      context.assignSplit(split, subtaskId);
    } else {
      log.info(
          String.format("========= handling split request from enumerator, subtask_id: %d, requester_hostname: %s",
              subtaskId, requesterHostname));

      int from = state.getCurrentValue();
      int until = from + 5;
      IntRangeSplit split = new IntRangeSplit(from, until, from);
      context.assignSplit(split, subtaskId);
      state.setCurrentValue(until);
    }
  }

  @Override
  public void addSplitsBack(List<IntRangeSplit> splits, int subtaskId) {
    log.info(String.format("========= adding split back from enumerator, subtask_id: %d, requester_hostname: %s",
        subtaskId, splits));

    for (IntRangeSplit split : splits) {
      log.info(String.format("+++++++ add split back data from: %d, until: %d, current: %d", split.getFrom(),
          split.getUntil(), split.getCurrentValue()));

      if (!state.getCompletedSplits().contains(split.splitId())) {
        state.getDeadSplits().add(split);
      }
    }
  }

  @Override
  public void addReader(int subtaskId) {
    log.info("========= adding reader from enumerator");
  }

  @Override
  public IntRangeEnumeratorState snapshotState(long checkpointId) {
    log.info(String.format("========= snapshoting enumerator state, checkpoint id: %d", checkpointId));

    return state;
  }

  @Override
  public void close() {
    log.info("========= closing enumerator");
  }

  @Override
  public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
    if (sourceEvent instanceof IntRangeCompletedSplit) {
      String splitId = ((IntRangeCompletedSplit) sourceEvent).getSplitId();
      state.getCompletedSplits().add(splitId);
    }
  }
}

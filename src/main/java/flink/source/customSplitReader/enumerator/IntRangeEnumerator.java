package flink.source.customSplitReader.enumerator;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import flink.source.customSplitReader.split.IntRangeSplit;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class IntRangeEnumerator implements SplitEnumerator<IntRangeSplit, IntRangeEnumeratorState> {
  private static final Logger log = LoggerFactory.getLogger(IntRangeEnumerator.class);

  private final SplitEnumeratorContext<IntRangeSplit> context;
  private final IntRangeEnumeratorState state;

  public IntRangeEnumerator(SplitEnumeratorContext<IntRangeSplit> context) {
    log.info("========= initializing custom split enumerator");

    // generate dummy topics for testing
    Set<Integer> topics = new HashSet<>(Arrays.asList(0, 50));
    this.context = context;
    this.state = new IntRangeEnumeratorState(topics);
  }

  public IntRangeEnumerator(SplitEnumeratorContext<IntRangeSplit> context, IntRangeEnumeratorState state) {
    log.info("========= initializing custom split enumerator with state");

    this.context = context;
    this.state = state;
  }

  @Override
  public void start() {
    log.info("========= starting enumerator");
  }

  @Override
  public void close() {
    log.info("========= closing enumerator");
  }

  @Override
  public void addReader(int subtaskId) {
    // Add logic to assign splits to readers
    log.info("========= adding reader from enumerator: " + subtaskId);

    for (Integer from : state.getSplitAssignment()) {
      IntRangeSplit split = new IntRangeSplit(from, from + 49, from);
      context.assignSplit(split, subtaskId);
      log.info("========= enumerator assigned split: {} to subtask: {}", split.splitId(), subtaskId);
    }
  }

  @Override
  public void addSplitsBack(List<IntRangeSplit> splits, int subtaskId) {
    log.info("========= adding splits back to reader: " + subtaskId);
  }

  @Override
  public void handleSplitRequest(int subtaskId, String requesterHostname) {
    log.info("========= handling split request for subtask: " + subtaskId);
  }

  @Override
  public IntRangeEnumeratorState snapshotState(long checkpointId) throws Exception {
    log.info("========= snapshotting enumerator state at checkpoint: " + checkpointId);
    return state;
  }
}

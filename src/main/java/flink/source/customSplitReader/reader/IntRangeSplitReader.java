package flink.source.customSplitReader.reader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import flink.source.customSplitReader.split.IntRangeSplit;

public class IntRangeSplitReader implements SplitReader<Integer, IntRangeSplit> {
  private static final Logger log = LoggerFactory.getLogger(IntRangeSplitReader.class);

  private List<IntRangeSplit> splits;
  private IntRangeSplit currentSplit;
  private int currentPosition;

  public IntRangeSplitReader() {
    log.info("========= initializing split reader");
    this.splits = new ArrayList<>();
  }

  @Override
  public RecordsWithSplitIds<Integer> fetch() throws IOException {
    log.info("========= fetching from split reader");

    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    List<Integer> records = new ArrayList<>();
    while (currentSplit != null && currentPosition <= currentSplit.getUntil()) {
      records.add(currentPosition++);
    }

    return new RecordsWithSplitIds<Integer>() {
      private boolean hasReturned = false;

      @Override
      public String nextSplit() {
        log.info("========= calling next split from split reader");
        if (!hasReturned && currentSplit != null) {
          hasReturned = true;
          return currentSplit.splitId();
        } else if (!hasReturned && !splits.isEmpty()) {
          currentSplit = splits.remove(0);
          currentPosition = currentSplit.getFrom();
          hasReturned = true;
          return currentSplit.splitId();
        }
        return null;
      }

      @Override
      public Integer nextRecordFromSplit() {
        log.info("========= calling next record from split from split reader");

        if (!records.isEmpty()) {
          return records.remove(0);
        }
        return null;
      }

      @Override
      public Set<String> finishedSplits() {
        log.info("========= calling finished splits from split reader");
        Set<String> finishedSplits = new HashSet<>();
        if (currentSplit != null && currentPosition > currentSplit.getUntil()) {
          finishedSplits.add(currentSplit.splitId());
          currentSplit = null;
        }

        return finishedSplits;
      }
    };
  }

  @Override
  public void handleSplitsChanges(SplitsChange<IntRangeSplit> splitsChange) {
    log.info(
        String.format("========= handling split change from split reader, %s", splitsChange.splits().get(0).splitId()));

    this.splits.addAll(splitsChange.splits());
    for (IntRangeSplit split : splitsChange.splits()) {
      log.info("========= registered split from split reader: " + split.splitId());
    }

    if (currentSplit == null && !this.splits.isEmpty()) {
      this.currentSplit = this.splits.remove(0);
      this.currentPosition = currentSplit.getFrom();
      log.info("========= split reader processing split: " + currentSplit.splitId());
    }
  }

  @Override
  public void wakeUp() {
    log.info("========= waking up split reader");
  }

  @Override
  public void close() throws Exception {
    log.info("========= closing split reader");
  }
}

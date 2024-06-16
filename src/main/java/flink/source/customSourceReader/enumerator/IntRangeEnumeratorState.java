package flink.source.customSourceReader.enumerator;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import flink.source.customSourceReader.split.IntRangeSplit;

public class IntRangeEnumeratorState implements Serializable {
  private int currentValue;
  private List<IntRangeSplit> deadSplits = new ArrayList<>();
  private Set<String> completedSplits = new HashSet<>();

  public IntRangeEnumeratorState(int currentValue) {
    this.currentValue = currentValue;
  }

  public int getCurrentValue() {
    return currentValue;
  }

  public void setCurrentValue(int currentValue) {
    this.currentValue = currentValue;
  }

  public List<IntRangeSplit> getDeadSplits() {
    return deadSplits;
  }

  public void setDeadSplits(List<IntRangeSplit> deadSplits) {
    this.deadSplits = deadSplits;
  }

  public Set<String> getCompletedSplits() {
    return completedSplits;
  }

  public void setCompletedSplits(Set<String> completedSplits) {
    this.completedSplits = completedSplits;
  }

  public byte[] serialize() throws IOException {
    try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(byteArrayOutputStream)) {
      out.writeObject(this);
      return byteArrayOutputStream.toByteArray();
    }
  }

  public static IntRangeEnumeratorState deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
    try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
        ObjectInputStream in = new ObjectInputStream(byteArrayInputStream)) {

      return (IntRangeEnumeratorState) in.readObject();
    }
  }
}

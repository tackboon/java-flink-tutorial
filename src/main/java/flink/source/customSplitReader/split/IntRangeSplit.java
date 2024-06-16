package flink.source.customSplitReader.split;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.apache.flink.api.connector.source.SourceSplit;

public class IntRangeSplit implements SourceSplit, Serializable {
  private final int from;
  private final int until;
  private int currentValue;

  public IntRangeSplit(int from, int until, int currentValue) {
    this.from = from;
    this.until = until;
    this.currentValue = currentValue;
  }

  @Override
  public String splitId() {
    return String.format("%d-%d", from, until);
  }

  public int getFrom() {
    return from;
  }

  public int getUntil() {
    return until;
  }

  public int getCurrentValue() {
    return currentValue;
  }

  public void setCurrentValue(int currentValue) {
    this.currentValue = currentValue;
  }

  public byte[] serialize() throws IOException {
    try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(byteArrayOutputStream)) {
      out.writeObject(this);
      return byteArrayOutputStream.toByteArray();
    }
  }

  public static IntRangeSplit deserialize(byte[] serialized) throws IOException, ClassNotFoundException {
    try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(serialized);
        ObjectInputStream in = new ObjectInputStream(byteArrayInputStream)) {
      return (IntRangeSplit) in.readObject();
    }
  }
}

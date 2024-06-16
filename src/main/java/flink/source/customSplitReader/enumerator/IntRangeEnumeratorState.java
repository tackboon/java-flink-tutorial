package flink.source.customSplitReader.enumerator;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Set;

public class IntRangeEnumeratorState implements Serializable {
  private final Set<Integer> splitAssignment;

  public IntRangeEnumeratorState(Set<Integer> splitAssignment) {
    this.splitAssignment = splitAssignment;
  }

  public Set<Integer> getSplitAssignment() {
    return splitAssignment;
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

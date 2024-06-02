package flink.app.customSource.split;

import java.io.IOException;

import org.apache.flink.core.io.SimpleVersionedSerializer;

public class IntRangeSplitSerializer implements SimpleVersionedSerializer<IntRangeSplit> {
  private final int CURRENT_VERSION = 1;

  @Override
  public int getVersion() {
    return CURRENT_VERSION;
  }

  @Override
  public byte[] serialize(IntRangeSplit split) throws IOException {
    return split.serialize();
  }

  @Override
  public IntRangeSplit deserialize(int version, byte[] serialized) throws IOException {
    if (version > CURRENT_VERSION) {
      throw new IOException(String.format(
          "this deserializer only supports version up to %d, but the bytes are serialized with version %d",
          CURRENT_VERSION, version));
    }

    try {
      return IntRangeSplit.deserialize(serialized);
    } catch (ClassNotFoundException e) {
      throw new IOException("class not found during deserizlization", e);
    }
  }
}

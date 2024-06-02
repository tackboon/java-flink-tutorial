package flink.app.customSource.enumerator;

import java.io.IOException;

import org.apache.flink.core.io.SimpleVersionedSerializer;

public class EnumeratorStateSerializer implements SimpleVersionedSerializer<EnumeratorState> {
  private final int CURRENT_VERSION = 1;

  @Override
  public int getVersion() {
    return CURRENT_VERSION;
  }

  @Override
  public byte[] serialize(EnumeratorState enumState) throws IOException {
    return enumState.serialize();
  }

  @Override
  public EnumeratorState deserialize(int version, byte[] serialized) throws IOException {
    if (version > CURRENT_VERSION) {
      throw new IOException(String.format(
          "this deserializer only supports version up to %d, but the bytes are serialized with version %d",
          CURRENT_VERSION, version));
    }

    try {
      return EnumeratorState.deserialize(serialized);
    } catch (ClassNotFoundException e) {
      throw new IOException("class not found during deserialization", e);
    }
  }
}

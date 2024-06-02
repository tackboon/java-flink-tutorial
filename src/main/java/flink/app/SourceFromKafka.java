package flink.app;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SourceFromKafka {
  public static void run() throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    KafkaSource<String> source = KafkaSource.<String>builder()
        .setBootstrapServers("host.docker.internal:9092")
        .setTopics("flink-test")
        .setGroupId("flink-group")
        .setStartingOffsets(OffsetsInitializer.latest())
        .setValueOnlyDeserializer(new SimpleStringSchema())
        .build();

    var stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka source");
    stream.print();

    env.execute("Source from kafka");
  }
}

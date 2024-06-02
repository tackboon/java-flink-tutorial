package flink.app;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SinkToKafka {
  public static void run() throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // source from socket
    var stream = env.socketTextStream("host.docker.internal", 9001);

    KafkaSink<String> sink = KafkaSink.<String>builder()
        .setBootstrapServers("host.docker.internal:9092")
        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
            .setTopic("flink-test")
            .setValueSerializationSchema(new SimpleStringSchema())
            .build())
        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build();

    stream.sinkTo(sink);

    env.execute("Sink to kafka");
  }
}

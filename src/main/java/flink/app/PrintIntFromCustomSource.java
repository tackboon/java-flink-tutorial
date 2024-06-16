package flink.app;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import flink.source.customSourceReader.IntRangeSource;

public class PrintIntFromCustomSource {
  public static void run() throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // checkpointing on every 5 minutes
    env.enableCheckpointing(1000 * 60 * 5);
    env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
    env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
    env.getCheckpointConfig().setCheckpointTimeout(60000);
    env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
    env.getCheckpointConfig()
        .setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

    env
        .fromSource(new IntRangeSource(), WatermarkStrategy.noWatermarks(), "int_range_source")
        .setParallelism(2)
        .print();

    env.execute("print int from custom source");
  }
}

package flink.app;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import flink.source.customSplitReader.IntRangeSource;

public class PrintIntFromCustomSplitReader {
  public static void run() throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    env
        .fromSource(new IntRangeSource(), WatermarkStrategy.noWatermarks(), "int_range_source")
        .setParallelism(2)
        .print();

    env.execute("print int from custom source");
  }
}

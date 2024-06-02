package flink.app;

import java.util.ArrayList;

import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Job execution like accumulators, runtime, etc. are not available on detached mode.
 * Please use cli to run the job in attached mode.
 */
public class CustomAggregator {
  private static final Logger log = LoggerFactory.getLogger(CustomAggregator.class);

  public static void run() throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    final String filepath = "/opt/flink/out/custom_aggregator";

    // define file sink
    final FileSink<String> sink = FileSink
        .forRowFormat(new Path(filepath), new SimpleStringEncoder<String>("UTF-8"))
        .withRollingPolicy(DefaultRollingPolicy.builder().build())
        .withOutputFileConfig(OutputFileConfig.builder().withPartPrefix("prefix").withPartSuffix(".txt").build())
        .build();

    var datas = new ArrayList<Integer>();
    datas.add(10);
    datas.add(15);
    datas.add(20);

    // source from collection
    var stream = env.fromData(datas);
    stream
        .map(new Aggregator())
        .setParallelism(2)
        .map(Object::toString)
        .sinkTo(sink);

    var res = env.execute("Custom aggregator");
    int count = res.getAccumulatorResult("custom-aggregator");

    log.info(String.format("total count: %d", count));
  }

  public static class Aggregator extends RichMapFunction<Integer, Integer> {
    private IntCounter counter = new IntCounter();

    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);

      getRuntimeContext().addAccumulator("custom-aggregator", counter);
    }

    @Override
    public Integer map(Integer value) throws Exception {
      counter.add(1);
      return value;
    }
  }
}

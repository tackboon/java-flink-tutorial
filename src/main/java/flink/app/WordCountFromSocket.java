package flink.app;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCountFromSocket {
  public static void run(String[] args) throws Exception {
    var params = ParameterTool.fromArgs(args);
    var port = params.getInt("port", 9000);
    var host = params.get("host", "host.docker.internal");

    if (host == null) {
      throw new IllegalArgumentException("socket host is required");
    }

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // source from socket
    var stream = env.socketTextStream(host, port);

    stream
        .flatMap(new Tokenizer())
        .keyBy(r -> r.f0)
        // .sum(1)
        .reduce(new Aggregator())
        .print();

    env.execute("Word Count with Socket");
  }

  public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
    @Override
    public void flatMap(String in, Collector<Tuple2<String, Integer>> out) throws Exception {
      var splits = in.split("\\s");
      for (String s : splits) {
        out.collect(new Tuple2<>(s, 1));
      }
    }
  }

  public static class Aggregator implements ReduceFunction<Tuple2<String, Integer>> {
    @Override
    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> a, Tuple2<String, Integer> b) {
      return new Tuple2<>(a.f0, a.f1 + b.f1);
    }
  }
}

package flink.app;

import java.util.ArrayList;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamFromData {
  public static void run() throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    var datas = new ArrayList<Integer>();
    datas.add(10);
    datas.add(15);
    datas.add(20);

    // source from collection
    var stream = env.fromData(datas);

    stream
        .map(new MapFunction<Integer, Integer>() {
          @Override
          public Integer map(Integer in) throws Exception {
            return ++in;
          }
        })
        .print();

    env.execute("Stream from Data");
  }
}

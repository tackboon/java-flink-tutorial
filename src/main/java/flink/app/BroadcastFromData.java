package flink.app;

import java.util.ArrayList;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class BroadcastFromData {
  public static void run() throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);

    var datas = new ArrayList<Integer>();
    datas.add(10);
    datas.add(15);
    datas.add(20);

    // source from collection
    var stream = env.fromData(datas);

    stream
        .broadcast()
        .map(new MapFunction<Integer, Integer>() {
          @Override
          public Integer map(Integer in) throws Exception {
            return ++in;
          }
        })
        .print();

    env.execute("Broadcast from Data");
  }
}

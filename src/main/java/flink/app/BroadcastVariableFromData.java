package flink.app;

import java.util.ArrayList;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class BroadcastVariableFromData {
  public static void run() throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    var variables = new ArrayList<Tuple2<String, Integer>>();
    variables.add(new Tuple2<>("a", 10));
    variables.add(new Tuple2<>("b", 20));
    variables.add(new Tuple2<>("c", 30));

    var broadcastStream = env
        .fromData(variables)
        .broadcast(new MapStateDescriptor<>("variables", Types.STRING, Types.INT));

    var stream = env.fromData("b", "a", "c");
    var keyStream = stream.keyBy(value -> value);

    keyStream
        .connect(broadcastStream)
        .process(new BroadcastFunction())
        .print();

    env.execute("Broadcast variable from Data");
  }

  public static class BroadcastFunction
      extends KeyedBroadcastProcessFunction<Void, String, Tuple2<String, Integer>, Integer> {

    private final MapStateDescriptor<String, Integer> broadcastStateDescriptor = new MapStateDescriptor<>("variables",
        Types.STRING, Types.INT);

    @Override
    public void processElement(String value, ReadOnlyContext ctx, Collector<Integer> out) throws Exception {
      ReadOnlyBroadcastState<String, Integer> broadcastState = ctx.getBroadcastState(broadcastStateDescriptor);
      if (broadcastState.contains(value)) {
        out.collect(broadcastState.get(value));
      }
    }

    @Override
    public void processBroadcastElement(Tuple2<String, Integer> value, Context ctx, Collector<Integer> out)
        throws Exception {
      BroadcastState<String, Integer> broadcastState = ctx.getBroadcastState(broadcastStateDescriptor);
      broadcastState.put(value.f0, value.f1);
    }
  }
}

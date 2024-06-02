package flink.app;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class WindowWatermarkEventTime {
  public static void run(String[] args) throws Exception {
    var params = ParameterTool.fromArgs(args);
    var port = params.getInt("port", 9000);
    var host = params.get("host", "host.docker.internal");

    if (host == null) {
      throw new IllegalArgumentException("socket host is required");
    }

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    final OutputTag<Tuple2<String, Long>> lateOutputTag = new OutputTag<>("late-data");

    // source from socket
    var stream = env.socketTextStream(host, port);

    stream
        .map(new InputMap())
        .assignTimestampsAndWatermarks(
            WatermarkStrategy
                .<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((event, timestamp) -> event.f1))
        .keyBy(data -> data.f0)
        .window(TumblingEventTimeWindows.of(Duration.ofSeconds(3)))
        .allowedLateness(Duration.ofSeconds(2))
        .sideOutputLateData(lateOutputTag)
        .apply(new WindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
          @Override
          public void apply(String key, TimeWindow window, Iterable<Tuple2<String, Long>> input, Collector<String> out)
              throws Exception {

            List<Long> eventTimes = new ArrayList<>();
            for (Tuple2<String, Long> next : input) {
              eventTimes.add(next.f1);
            }

            // sort the datas
            Collections.sort(eventTimes);

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

            out.collect(String.format(
                "key: %s, size: %d, smallest time: %s, biggest time: %s, window start: %s, window end: %s",
                key, eventTimes.size(), sdf.format(eventTimes.get(0)),
                sdf.format(eventTimes.get(eventTimes.size() - 1)),
                sdf.format(window.getStart()), sdf.format(window.getEnd())));
          }
        })
        .print();

    var lateStream = stream.getSideOutput(lateOutputTag);
    lateStream
        .map(data -> "Late data: " + data)
        .print();

    env.execute("Window Watermark event time");
  }

  public static class InputMap implements MapFunction<String, Tuple2<String, Long>> {
    @Override
    public Tuple2<String, Long> map(String value) throws Exception {
      String[] arr = value.split(",");
      return new Tuple2<>(arr[0], Long.parseLong(arr[1]));
    }
  }
}

package flink.app;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Map;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DistributiveCache {
  public static void run() throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // make sure the file is in job manager
    env.registerCachedFile("/opt/flink/conf/cache.json", "cache.json");

    var datas = new ArrayList<String>();
    datas.add("key1");
    datas.add("key2");
    datas.add("key3");

    // source from collection
    var stream = env.fromData(datas);

    stream
        .map(new CacheMap())
        .print();

    env.execute("Distributive cache");
  }

  public static class CacheMap extends RichMapFunction<String, String> {
    private transient Map<String, Object> cacheData;

    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);

      // retrive the cache file
      File cacheFile = getRuntimeContext().getDistributedCache().getFile("cache.json");

      // read the json file into a map
      BufferedReader reader = new BufferedReader(new FileReader(cacheFile));
      ObjectMapper objectMapper = new ObjectMapper();
      cacheData = objectMapper.readValue(reader, new TypeReference<Map<String, Object>>() {
      });
      reader.close();
    }

    @Override
    public String map(String key) throws Exception {
      Object value = cacheData.get(key);
      return key + ": " + value;
    }
  }
}

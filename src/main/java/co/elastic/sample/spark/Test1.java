package co.elastic.sample.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class Test1 {
    private static Random r = new Random();
    public static void main(String[] args)  {
        JavaSparkContext context = new JavaSparkContext(new SparkConf().setAppName("Test1"));
        int docCount = 100;
        List<Map<String, ?>> docs = new ArrayList<>(docCount);
        for(int i = 0; i <= docCount; i++){
            HashMap<String, Object> map = new HashMap<>();
            map.put("count", i);
            map.put("mytype", r.nextBoolean() ? "foo" : "bar");
            map.put("@timestamp", ZonedDateTime.now( ZoneOffset.UTC ).format( DateTimeFormatter.ISO_INSTANT ));
            docs.add(map);
        }
        JavaEsSpark.saveToEs(context.parallelize(docs), "my-index-{mytype}-{@timestamp|yyy.MM.dd}");
        context.stop();
    }
}


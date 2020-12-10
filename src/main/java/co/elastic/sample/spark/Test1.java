package co.elastic.sample.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Test1 {
    public static void main(String[] args)  {
        JavaSparkContext context = new JavaSparkContext(new SparkConf().setAppName("Test1"));
        int docCount = 100;
        List<Map<String, Integer>> docs = new ArrayList<>(docCount);
        for(int i = 0; i <= docCount; i++){
            HashMap<String, Integer> map = new HashMap<>();
            map.put("doc", i);
            docs.add(map);
        }
        JavaEsSpark.saveToEs(context.parallelize(docs), "testindex1");
        context.stop();
    }
}


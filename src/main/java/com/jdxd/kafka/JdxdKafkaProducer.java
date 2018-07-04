package com.jdxd.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Properties;

public class JdxdKafkaProducer implements Serializable {
    private static String path_click = "/Users/pangw/pw_workspace/IdeaProjects/course_jdxd/data/jdxd/t_click";
    private static String masterUrl = "local[1]";
    private static String topic = "test";


    public static void main(String[] args) {


        SparkConf conf = new SparkConf().setMaster(masterUrl)
                .setAppName("wordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> clickRDD = sc.textFile(path_click);
        JavaPairRDD<String, String> t_clickRDD = clickRDD
                .mapToPair(new PairFunction<String, String, String>() {
                    @Override
                    public Tuple2<String, String> call(String s) throws Exception {
                        String[] lines = s.split(",");
                        return new Tuple2(lines[0], lines[1] + "," + lines[2]);
                    }
                });
        t_clickRDD.foreach(new VoidFunction<Tuple2<String, String>>() {
            @Override
            public void call(Tuple2<String, String> stringStringTuple2) throws Exception {

                Properties props = getConfig();
                Producer<String, String> producer = new KafkaProducer<String, String>(props);

                String key = stringStringTuple2._1();
                String value = stringStringTuple2._2();
                System.out.println("key:" + key + "------" + "value:" + value);
//                producer.send(new ProducerRecord<String, String>(topic, key, value));
                producer.send(new ProducerRecord<String, String>(topic, "fdsfds", "fffff"));

                Thread.sleep(1000);
            }
        });


        //关闭
//        jp.closeProduce();

    }

    public static Properties getConfig() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "bigdata:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }
}

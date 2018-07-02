package com.jdxd.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class JavaWorldCount {
    public static void main (String[] args){
        System.out.println("大家好才是真的好");

        String masterUrl = "local[1]";
        String inputFile = "/Users/pangw/Downloads/bigdata/course_jdxd/data/jdxd/t_click/";
        String outputFile = "/tmp/output";

        if (args.length > 0) {
            masterUrl = args[0];
        } else if(args.length > 2) {
            inputFile = args[1];
            outputFile = args[2];
        }
        SparkConf conf = new SparkConf().setMaster(masterUrl)
                .setAppName("wordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> input = sc.textFile(inputFile);

        JavaRDD<String> words = input.flatMap(
                new FlatMapFunction<String, String>() {
                    @Override
                    public Iterator<String> call(String s) throws Exception {
                        return Arrays.asList(s.split(",")).iterator();
                    }
                }
        ).filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                return s.length()>1;
            }
        });

        JavaPairRDD<String,Integer> counts = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) throws Exception {
                        return new Tuple2(s,1);
                    }
                }
        ).reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer x, Integer y) throws Exception {
                        return x+y;
                    }
                }
        );

        counts.foreach(
                new VoidFunction<Tuple2<String, Integer>>() {
                    @Override
                    public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                        System.out.println(String.format("%s - %d", stringIntegerTuple2._1(), stringIntegerTuple2._2()));
                    }
                }
        );



    }
}

package com.jdxd.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Int;
import scala.Tuple2;

/**
 * 1、借款金额超过 200 且购买商品总价值超过借款总金额的用户 ID
 * 2、从不买打折产品且不借款的用户 ID
 */
public class JavaLoanAnalyze {
    private static String path_click = "/Users/pangw/pw_workspace/IdeaProjects/course_jdxd/data/jdxd/t_click";
    private static String path_loan = "/Users/pangw/pw_workspace/IdeaProjects/course_jdxd/data/jdxd/t_loan";
    private static String path_loan_sum = "/Users/pangw/pw_workspace/IdeaProjects/course_jdxd/data/jdxd/t_loan_sum";
    private static String path_order = "/Users/pangw/pw_workspace/IdeaProjects/course_jdxd/data/jdxd/t_order";
    private static String path_user = "/Users/pangw/pw_workspace/IdeaProjects/course_jdxd/data/jdxd/t_user";
    static String masterUrl = "local[1]";

    public static void main(String[] args) {
        System.out.println("大家好才是真的");
        SparkConf conf = new SparkConf().setMaster(masterUrl)
                .setAppName("wordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> orderRDD = sc.textFile(path_order);
        JavaRDD<String> loanRDD = sc.textFile(path_loan);
        JavaRDD<String> userRDD = sc.textFile(path_user);


        JavaPairRDD<String, Double> discont_users = orderRDD
                .mapToPair(new PairFunction<String, String, Double>() {
                    @Override
                    public Tuple2<String, Double> call(String s) throws Exception {
                        String[] lines = s.split(",");
                        Double d = 0.0;
                        try {
                            d = Double.valueOf(lines[5]);
                        } catch (Exception e) {
                        }
                        return new Tuple2(lines[0], d);
                    }
                });
        JavaPairRDD<String, Double> loan_users = loanRDD
                .mapToPair(new PairFunction<String, String, Double>() {
                    @Override
                    public Tuple2<String, Double> call(String s) throws Exception {
                        String[] lines = s.split(",");
                        Double d = 0.0;
                        try {
                            d = Double.valueOf(lines[2]);
                        } catch (Exception e) {
                        }
                        return new Tuple2(lines[0], d);
                    }
                });
        JavaPairRDD<String, Double> userRDD_all = userRDD
                .mapToPair(new PairFunction<String, String, Double>() {
                    @Override
                    public Tuple2<String, Double> call(String s) throws Exception {
                        String[] lines = s.split(",");
                        return new Tuple2(lines[0], 0.0);
                    }
                })
                .join(discont_users)
                .mapToPair(new PairFunction<Tuple2<String, Tuple2<Double, Double>>, String, Double>() {
                    @Override
                    public Tuple2<String, Double> call(Tuple2<String, Tuple2<Double, Double>> stringTuple2Tuple2) throws Exception {
                        Double d = stringTuple2Tuple2._2()._1() + stringTuple2Tuple2._2()._2();
                        return new Tuple2(stringTuple2Tuple2._1(), d);
                    }
                })
                .join(loan_users)
                .mapToPair(new PairFunction<Tuple2<String, Tuple2<Double, Double>>, String, Double>() {
                    @Override
                    public Tuple2<String, Double> call(Tuple2<String, Tuple2<Double, Double>> stringTuple2Tuple2) throws Exception {

                        Double d = stringTuple2Tuple2._2()._1() + stringTuple2Tuple2._2()._2();
                        return new Tuple2(stringTuple2Tuple2._1(), d);
                    }
                })
                .filter(new Function<Tuple2<String, Double>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Double> stringDoubleTuple2) throws Exception {

                        return stringDoubleTuple2._2() == 0;
                    }
                });
        userRDD_all.foreach(new VoidFunction<Tuple2<String, Double>>() {
            @Override
            public void call(Tuple2<String, Double> stringDoubleTuple2) throws Exception {
                System.out.println(stringDoubleTuple2._1() + "-------" + stringDoubleTuple2._2());
            }
        });


        //提取(用户id、购买价格)
        JavaPairRDD<String, String> id_price = orderRDD
                .mapToPair(
                        new PairFunction<String, String, String>() {
                            @Override
                            public Tuple2<String, String> call(String s) throws Exception {
                                String[] line = s.split(",");
                                String key = "00";
                                String value = "0.00";
                                if (!line[0].equals("")) {
                                    key = line[0];
                                }
                                if (!line[2].equals("")) {
                                    value = line[2];
                                }
                                return new Tuple2(key, value);
                            }
                        }
                ).reduceByKey(
                        new Function2<String, String, String>() {
                            @Override
                            public String call(String s, String s2) throws Exception {
                                return (Double.valueOf(s) + Double.valueOf(s2)) + "";
                            }
                        }
                );
        //用户id  借款金额
        JavaPairRDD<String, String> id_loans = loanRDD.mapToPair(
                new PairFunction<String, String, String>() {
                    @Override
                    public Tuple2<String, String> call(String s) throws Exception {
                        String[] line = s.split(",");
                        return new Tuple2(line[0], line[2]);
                    }
                }
        ).reduceByKey(new Function2<String, String, String>() {
            @Override
            public String call(String s, String s2) throws Exception {
                Double dd = 0.0;
                try {
                    Double d1 = Double.valueOf(s);
                    Double d2 = Double.valueOf(s2);
                    dd = d1 + d2;
                } catch (Exception e) {

                }
                System.out.println("借款金额：" + dd);
                return dd + "";
            }
        });
        //合并(<id<prices,loans>>)
        JavaPairRDD<String, Tuple2<String, String>> price_loans = id_price
                .join(id_loans)
                .filter(new Function<Tuple2<String, Tuple2<String, String>>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Tuple2<String, String>> stringTuple2Tuple2) throws Exception {
                        Boolean b = false;
                        try {
                            if (Double.valueOf(stringTuple2Tuple2._2()._2()) > 200) {
                                b = true;
                            }
                        } catch (Exception e) {
                        }
                        return b;
                    }
                })
                .filter(new Function<Tuple2<String, Tuple2<String, String>>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Tuple2<String, String>> stringTuple2Tuple2) throws Exception {
                        return Double.valueOf(stringTuple2Tuple2._2()._1()) > Double.valueOf(stringTuple2Tuple2._2()._2());
                    }
                });
        //输出 （1、借款金额超过 200 且购买商品总价值超过借款总金额的用户 ID）
//        price_loans.foreach(new VoidFunction<Tuple2<String, Tuple2<String, String>>>() {
//            @Override
//            public void call(Tuple2<String, Tuple2<String, String>> stringTuple2Tuple2) throws Exception {
//                System.out.println(stringTuple2Tuple2._1() + "---" + stringTuple2Tuple2._2()._1() + "---" + stringTuple2Tuple2._2()._2());
//            }
//        });


    }
}

package com.jdxd.spark.sql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * 1、借款金额超过 200 且购买商品总价值超过借款总金额的用户 ID
 * 2、
 */
public class JavaSparkSqlAnalyze {
    private static String path_click = "/Users/pangw/pw_workspace/IdeaProjects/course_jdxd/data/jdxd/t_click";
    private static String path_loan = "/Users/pangw/pw_workspace/IdeaProjects/course_jdxd/data/jdxd/t_loan";
    private static String path_loan_sum = "/Users/pangw/pw_workspace/IdeaProjects/course_jdxd/data/jdxd/t_loan_sum";
    private static String path_order = "/Users/pangw/pw_workspace/IdeaProjects/course_jdxd/data/jdxd/t_order";
    private static String path_user = "/Users/pangw/pw_workspace/IdeaProjects/course_jdxd/data/jdxd/t_user";

    public static void main(String[] args) {
        System.out.println("hfhdshfdhsf");

        SparkSession spark = SparkSession.builder()
                .master("local")
                .getOrCreate();
//        Dataset<Row> loans = spark.read().format("csv").load(path_loan);
        Dataset<Row> loans = spark.read().format("csv").option("header", "true").load(path_loan);
        Dataset<Row> order = spark.read().format("csv").option("header", "true").load(path_order);


        Dataset<Row> uid_amounts = loans.groupBy("uid")
                .agg(functions.sum("loan_amount"))
                .filter("sum(loan_amount)>200");

        Dataset<Row> uid_order = order.select("uid", "price")
                .groupBy("uid")
                .agg(functions.sum("price"));

        Dataset<Row> uid_finly = uid_amounts.join(uid_order,"uid");

//                .filter("sum(loan_amount)<sum(price)");

        uid_finly.show();

        uid_finly.filter(functions.col("sum(loan_amount)").gt(functions.col("sum(price)")))
                .select("uid")
                .foreach(new ForeachFunction<Row>() {
                    @Override
                    public void call(Row row) throws Exception {
                        System.out.println(row.get(0));
                    }
                });

//        loans.groupBy("_c0").agg(functions.count("_c2")).show();
//        long a = loans.filter("_c0==77371").count();
//        System.out.println("77371的数量是："+a);
        spark.stop();

    }
}

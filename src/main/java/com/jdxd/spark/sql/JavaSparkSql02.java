package com.jdxd.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * 从不买打折产品且不借款的用户 ID
 */
public class JavaSparkSql02 {

    private static String path_click = "/Users/pangw/pw_workspace/IdeaProjects/course_jdxd/data/jdxd/t_click";
    private static String path_loan = "/Users/pangw/pw_workspace/IdeaProjects/course_jdxd/data/jdxd/t_loan";
    private static String path_loan_sum = "/Users/pangw/pw_workspace/IdeaProjects/course_jdxd/data/jdxd/t_loan_sum";
    private static String path_order = "/Users/pangw/pw_workspace/IdeaProjects/course_jdxd/data/jdxd/t_order";
    private static String path_user = "/Users/pangw/pw_workspace/IdeaProjects/course_jdxd/data/jdxd/t_user";

    public static void main(String[] args){
        SparkSession spark = SparkSession.builder()
                .master("local")
                .getOrCreate();

        Dataset<Row> user = spark.read().format("csv").option("header","true").load(path_user);

        Dataset<Row> order = spark.read().format("csv").option("header","true").load(path_order);

        Dataset<Row> loan = spark.read().format("csv").option("header","true").load(path_loan);















    }
}

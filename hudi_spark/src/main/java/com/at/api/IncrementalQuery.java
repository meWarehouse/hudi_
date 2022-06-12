package com.at.api;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

/**
 * @create 2022-06-12
 */
public class IncrementalQuery {

    public static void main(String[] args) {

        // D:\\workspace\\hudi_\\hudi_spark\\files\\tbl_trips_cow tbl_trips_cow COPY_ON_WRITE -Xms 100m -Xmx 100m

        if (args.length < 3) {
            System.err.println("Usage: HoodieWriteClientExample <tablePath> <tableName>");
            System.exit(1);
        }

        String tablePath = args[0];
        String tableName = args[1];
        String tableType = args[2];


        SparkConf sparkConf = new SparkConf()
                .setAppName("Insert Hudi")
                .setMaster("local[*]")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryoserializer.buffer.max", "512m");


        // 创建SparkSession实例对象，设置属性
        SparkSession spark = new SparkSession
                .Builder()
                .config(sparkConf)
                .getOrCreate();


        try (JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext())){

            Dataset<Row> roViewDF = spark
                    .read()
                    .format("org.apache.hudi")
                    .load(tablePath + "/*/*/*/*");

            roViewDF.createOrReplaceTempView("hudi_ro_table");

            List<String> commits = spark
                    .sql("select distinct(_hoodie_commit_time) as commitTime from hudi_ro_table order by commitTime")
                    .toJavaRDD()
                    .map((Function<Row, String>) row -> row.toString())
                    .take(50);

            // commit time we are interested in
            String beginTime = commits.get(commits.size() - 2);

            // incrementally query data
            Dataset<Row> incViewDF = spark
                    .read()
                    .format("org.apache.hudi")
                    .option("hoodie.datasource.query.type", "incremental")
                    .option("hoodie.datasource.read.begin.instanttime", beginTime)
                    .load(tablePath);

            incViewDF.createOrReplaceTempView("hudi_incr_table");

            spark.sql("select `_hoodie_commit_time`, fare, begin_lon, begin_lat, ts from hudi_incr_table ")
                    .show();




        }



    }

}

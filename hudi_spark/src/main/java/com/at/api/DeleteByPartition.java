package com.at.api;

import org.apache.hudi.QuickstartUtils;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.hudi.config.HoodieWriteConfig.TBL_NAME;
import static org.apache.spark.sql.SaveMode.Append;

/**
 * @create 2022-06-12
 */
public class DeleteByPartition {

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


        try (JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext())) {

            Dataset<Row> roViewDF = spark
                    .read()
                    .format("org.apache.hudi")
                    .load(tablePath + "/*/*/*/*");

            roViewDF.createOrReplaceTempView("hudi_ro_table");

            spark.sql(
                    "select _hoodie_commit_time, _hoodie_record_key, _hoodie_partition_path,begin_lat, begin_lon, driver, end_lat, end_lon, fare, partitionpath, rider, ts, uuid from  hudi_ro_table")
                    .show();


            Dataset<Row> df = spark.emptyDataFrame();
            df.write().format("org.apache.hudi")
                    .option("hoodie.insert.shuffle.parallelism", "2")
                    .option("hoodie.upsert.shuffle.parallelism", "2")
                    .option("hoodie.bulkinsert.shuffle.parallelism", "2")
                    .option("hoodie.delete.shuffle.parallelism", "2")
                    .option(HoodieWriteConfig.PRECOMBINE_FIELD_NAME.key(), "ts")
                    .option(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "uuid")
                    .option(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "partitionpath")
                    .option(TBL_NAME.key(), tableName)
                    .option("hoodie.datasource.write.operation", WriteOperationType.DELETE.value())
                    .option("hoodie.datasource.write.partitions.to.delete",
                            String.join(", ", HoodieExampleDataGenerator.DEFAULT_PARTITION_PATHS))
                    .mode(Append)
                    .save(tablePath);

            spark.sql(
                    "select _hoodie_commit_time, _hoodie_record_key, _hoodie_partition_path,begin_lat, begin_lon, driver, end_lat, end_lon, fare, partitionpath, rider, ts, uuid from  hudi_ro_table")
                    .show();

        }


    }


}

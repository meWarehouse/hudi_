package com.at.api;

import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.List;

/**
 * @create 2022-06-12
 */
public class Update {

    public static void main(String[] args) {

        if (args.length < 3) {
            System.err.println("Usage: HoodieWriteClientExample <tablePath> <tableName>");
            System.exit(1);
        }

        String tablePath = args[0];
        String tableName = args[1];
        String tableType = args[2];

        SparkConf sparkConf = new SparkConf()
                .setAppName("hudi update")
                .setMaster("local[2]")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryoserializer.buffer.max", "512m");


        SparkSession spark = new SparkSession
                .Builder()
                .config(sparkConf)
                .getOrCreate();


        try (JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext())) {

            HoodieExampleDataGenerator<HoodieAvroPayload> dataGen = new HoodieExampleDataGenerator<>();
            String commitTime = Long.toString(System.currentTimeMillis());

            List<String> insertData = dataGen.convertToStringList(dataGen.generateInserts(commitTime, 20));

            System.out.println("insert data ==========================================================");
            insertData.stream().forEach(r -> System.out.println(r));
            System.out.println("insert data ==========================================================");

            Dataset<Row> df = spark.read().json(jsc.parallelize(insertData, 1));

            System.out.println("============================ insert ============================");

            df
                    .write()
                    .format("org.apache.hudi")
                    .option("hoodie.insert.shuffle.parallelism", "2")
                    .option("hoodie.upsert.shuffle.parallelism", "2")
                    .option("hoodie.bulkinsert.shuffle.parallelism", "2")
                    .option("hoodie.delete.shuffle.parallelism", "2")
                    .option(HoodieWriteConfig.PRECOMBINE_FIELD_NAME.key(), "ts") //数据更新时间戳的
                    .option(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "uuid") //设置主键
                    .option(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "partitionpath") //hudi分区列
                    .option(HoodieWriteConfig.TBL_NAME.key(), tableName) //hudi表名
                    .mode(SaveMode.Overwrite)
                    .save(tablePath);

//            df.select("begin_lat", "begin_lon", "driver", "end_lat", "end_lon", "fare", "partitionpath", "rider", "ts", "uuid").show(20, false);
            df.select("begin_lat", "begin_lon", "driver", "end_lat", "end_lon", "fare", "partitionpath", "rider", "ts", "uuid").show();


            System.out.println("============================ update ============================");
            List<String> updateData = dataGen.convertToStringList(dataGen.generateInserts(commitTime, 10));
            Dataset<Row> updateDf = spark.read().json(jsc.parallelize(updateData, 1));

            updateDf
                    .write()
                    .format("org.apache.hudi")
                    .option("hoodie.insert.shuffle.parallelism", "2")
                    .option("hoodie.upsert.shuffle.parallelism", "2")
                    .option("hoodie.bulkinsert.shuffle.parallelism", "2")
                    .option("hoodie.delete.shuffle.parallelism", "2")
                    .option(HoodieWriteConfig.PRECOMBINE_FIELD_NAME.key(), "ts") //数据更新时间戳的
                    .option(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "uuid") //设置主键
                    .option(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "partitionpath") //hudi分区列
                    .option(HoodieWriteConfig.TBL_NAME.key(), tableName) //hudi表名
                    .mode(SaveMode.Append)
                    .save(tablePath);

            Dataset<Row> roViewDF = spark
                    .read()
                    .format("org.apache.hudi")
                    .load(tablePath + "/*/*/*/*");

            roViewDF.createOrReplaceTempView("hudi_ro_table");

//            spark.sql("select fare, begin_lon, begin_lat, ts from  hudi_ro_table where fare > 20.0").show();

            df.select("begin_lat", "begin_lon", "driver", "end_lat", "end_lon", "fare", "partitionpath", "rider", "ts", "uuid").show();

            spark.sql(
                    "select _hoodie_commit_time, _hoodie_record_key, _hoodie_partition_path,begin_lat, begin_lon, driver, end_lat, end_lon, fare, partitionpath, rider, ts, uuid from  hudi_ro_table")
                    .show();


        }


    }


}

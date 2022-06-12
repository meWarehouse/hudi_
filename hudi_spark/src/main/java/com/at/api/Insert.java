package com.at.api;


import org.apache.hudi.QuickstartUtils;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;


import java.util.List;

/**
 * @create 2022-06-11
 */
public class Insert {


    public static void main(String[] args) {
//
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

        // 定义变量：表名称、保存路径
//        String tableName = "tbl_trips_cow";
//        String tablePath = "D:\\workspace\\hudi_\\hudi_spark\\files\\tbl_trips_cow";


        //HoodieRecord{key=HoodieKey { recordKey=b2e0c338-db33-4c59-8048-be6c12ebe87e partitionPath=2020/01/02}, currentLocation='null', newLocation='null'}
        HoodieExampleDataGenerator<HoodieAvroPayload> dataGen = new HoodieExampleDataGenerator<>();


        String commitTime = Long.toString(System.currentTimeMillis());

        // {"ts": 0, "uuid": "d9e1fd18-116c-4f01-befb-67ec5df019df", "rider": "rider-1655041662901", "driver": "driver-1655041662901", "begin_lat": 0.15330847537835646, "begin_lon": 0.1962305768406577, "end_lat": 0.36964170578655997, "end_lon": 0.2110206104048945, "fare": 27.83086084578943, "partitionpath": "2020/01/02"}
        List<String> insertData = dataGen.convertToStringList(dataGen.generateInserts(commitTime, 20));

        System.out.println("insert data ==========================================================");
        insertData.stream().forEach(r -> System.out.println(r));
        System.out.println("insert data ==========================================================");


        try (JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext())) {

            JavaRDD<String> stringJavaRDD = jsc.parallelize(insertData, 1);
            Dataset<Row> df = spark.read().json(stringJavaRDD);


            df
                    .write()
                    .format("org.apache.hudi")
                    .option("hoodie.insert.shuffle.parallelism", "2")
                    .option("hoodie.upsert.shuffle.parallelism", "2")
                    .option("hoodie.bulkinsert.shuffle.parallelism", "2")
                    .option("hoodie.delete.shuffle.parallelism", "2")
//                    .option("hoodie.table.type","COPY_ON_WRITE1") //COPY_ON_WRITE MERGE_ON_READ
//                    .option("table.type","MERGE_ON_READ") //COPY_ON_WRITE MERGE_ON_READ
                    .option("hoodie.table.type", HoodieTableType.valueOf(tableType).name())
                    .option(HoodieWriteConfig.PRECOMBINE_FIELD_NAME.key(), "ts") //数据更新时间戳的
                    .option(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "uuid") //设置主键
                    .option(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "partitionpath") //hudi分区列
                    .option(HoodieWriteConfig.TBL_NAME.key(), tableName) //hudi表名
                    .mode(SaveMode.Overwrite)
                    .save(tablePath);


            df.select("begin_lat","begin_lon","driver","end_lat","end_lon","fare","partitionpath","rider","ts","uuid").show(20,false);


        }


    }


}



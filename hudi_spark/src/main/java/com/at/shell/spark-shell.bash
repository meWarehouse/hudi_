

./bin/spark-shell \
--jars /opt/module/hudi/hudi-0.11.0/packaging/hudi-spark-bundle/target/hudi-spark3-bundle_2.12-0.11.0.jar \
--packages org.apache.spark:spark-avro_2.12:3.0.1 \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer'


# spark-shell
# 导入Spark及Hudi相关包和定义变量（表的名称和数据存储路径）
import org.apache.hudi.QuickstartUtils._
import scala.collection.JavaConversions._
import org.apache.spark.sql.SaveMode._
import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig._

val tableName = "hudi_trips_cow"
val basePath = "file:///tmp/hudi_trips_cow"
val dataGen = new DataGenerator


# 构建DataGenerator对象，用于模拟生成Trip乘车数据
val inserts = convertToStringList(dataGen.generateInserts(10))
# 生成数据的格式
#{
#    "ts":1654457502965,
#    "uuid":"2888aa5d-91df-406e-89eb-7bbad6eb982e",
#    "rider":"rider-213",
#    "driver":"driver-213",
#    "begin_lat":0.4726905879569653,
#    "begin_lon":0.46157858450465483,
#    "end_lat":0.754803407008858,
#    "end_lon":0.9671159942018241,
#    "fare":34.158284716382845,
#    "partitionpath":"americas/brazil/sao_paulo"
#}

# 将模拟数据List转换为DataFrame数据集
val df = spark.read.json(spark.sparkContext.parallelize(inserts, 2))

# 查看转换后DataFrame数据集的Schema信息
df.printSchema()
# Schema信息
#root
# |-- begin_lat: double (nullable = true)
# |-- begin_lon: double (nullable = true)
# |-- driver: string (nullable = true)
# |-- end_lat: double (nullable = true)
# |-- end_lon: double (nullable = true)
# |-- fare: double (nullable = true)
# |-- partitionpath: string (nullable = true)
# |-- rider: string (nullable = true)
# |-- ts: long (nullable = true)
# |-- uuid: string (nullable = true)


# 查看模拟样本数据
df.select("begin_lat","begin_lon","driver","end_lat","end_lon","fare","partitionpath","rider","ts","uuid").show(3,truncate=false)
#+------------------+-------------------+----------+-------------------+-------------------+------------------+------------------------------------+---------+-------------+------------------------------------+
#|begin_lat         |begin_lon          |driver    |end_lat            |end_lon            |fare              |partitionpath                       |rider    |ts           |uuid                                |
#+------------------+-------------------+----------+-------------------+-------------------+------------------+------------------------------------+---------+-------------+------------------------------------+
#|0.4726905879569653|0.46157858450465483|driver-213|0.754803407008858  |0.9671159942018241 |34.158284716382845|americas/brazil/sao_paulo           |rider-213|1654826306467|6014b2be-ee56-4448-b201-e2dfa84aec81|
#|0.6100070562136587|0.8779402295427752 |driver-213|0.3407870505929602 |0.5030798142293655 |43.4923811219014  |americas/brazil/sao_paulo           |rider-213|1654891549495|3650c236-d94c-4cea-87bc-fd904bc13729|
#|0.5731835407930634|0.4923479652912024 |driver-213|0.08988581780930216|0.42520899698713666|64.27696295884016 |americas/united_states/san_francisco|rider-213|1654670232080|41314b92-ba87-4f89-b924-46b390a2b8ea|
#+------------------+-------------------+----------+-------------------+-------------------+------------------+------------------------------------+---------+-------------+------------------------------------+
#

# 将模拟产生Trip数据，保存到Hudi表中
df.write.format("hudi").
  options(getQuickstartWriteConfigs).
  option(PRECOMBINE_FIELD_OPT_KEY, "ts").
  option(RECORDKEY_FIELD_OPT_KEY, "uuid").
  option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
  option(TABLE_NAME, tableName).
  mode(Overwrite).
  save(basePath)











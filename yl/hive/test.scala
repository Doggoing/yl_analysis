package com.yl.hive

import com.gy.hive.extract.config.Configurtion
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object test {
  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", Configurtion.SYSTEM_PROXY_USER)
    System.setProperty("hadoop.home.dir", Configurtion.LOCAL_HADOOP_HOME)

    //TODO 1.准备环境
    val conf: SparkConf = new SparkConf()
      .setMaster("local[*]") //虚拟机上运行换yarn
      .setAppName(this.getClass.getSimpleName)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val spark: SparkSession = SparkSession.builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    // Step 2: Read CSV data
    val filePath = "cardio_train.csv" // Update with actual file path
    val df = spark.read.option("header", "true").csv(filePath)

    // Step 3: Show schema and sample data (optional for exploration)
    df.printSchema()
    df.show(5)

    // Step 4: Data Preprocessing (handling missing values, cleaning data)
    val cleanDF = df
      .withColumn("age", col("age").cast("int"))
      .withColumn("height", col("height").cast("float"))
      .withColumn("weight", col("weight").cast("float"))
      .withColumn("ap_hi", col("ap_hi").cast("int"))
      .withColumn("ap_lo", col("ap_lo").cast("int"))
      .withColumn("cholesterol", col("cholesterol").cast("int"))
      .withColumn("gluc", col("gluc").cast("int"))
      .withColumn("smoke", col("smoke").cast("int"))
      .withColumn("alco", col("alco").cast("int"))
      .withColumn("active", col("active").cast("int"))
      .withColumn("cardio", col("cardio").cast("int"))

    // Handling missing values: Fill missing values with default values
    val filledDF = cleanDF.na.fill(Map(
      "age" -> 0, // Default age value
      "height" -> 0.0f,
      "weight" -> 0.0f,
      "ap_hi" -> 0,
      "ap_lo" -> 0,
      "cholesterol" -> 1, // Assuming 1 is normal cholesterol
      "gluc" -> 1, // Assuming 1 is normal glucose
      "smoke" -> 0, // Assuming 0 means no smoking
      "alco" -> 0, // Assuming 0 means no alcohol consumption
      "active" -> 0, // Assuming 0 means not active
      "cardio" -> 0 // Assuming no cardiovascular disease
    ))

    // Step 5: Data Transformation (Optional: Create new features, calculations)
    val transformedDF = filledDF
      .withColumn("bmi", (col("weight") / (col("height") / 100) * (col("height") / 100)))

    // Step 6: Select the desired columns for final table
    val finalDF = transformedDF.select("id", "age", "gender", "height", "weight", "ap_hi", "ap_lo", "cholesterol", "gluc", "smoke", "alco", "active", "cardio", "bmi")

    // Step 7: Show cleaned and transformed data
    finalDF.show(5)


    // TODO : 02.获取mysql数据

    // TODO : 03.获取hive or hudi数据
    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    // TODO : 04.数据处理



    // TODO : 05.数据写入

    spark.close()

  }



}

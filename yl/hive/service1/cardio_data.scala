package com.yl.hive.service1

import com.gy.hive.extract.config.Configurtion
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, current_timestamp, date_format, date_sub, round}

import java.util.Properties

object cardio_data {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]")
      .setAppName("全量加载数据")

    val spark = SparkSession.builder()
      .config(conf)
      //.config("hive.metastore.uris", "thrift://192.168.201.10:9083")
      .enableHiveSupport()
      .getOrCreate()

    //数据库连接地址
    val url = "jdbc:mysql://localhost:3306/heart_test?characterEncoding=UTF-8"
    //数据库连接参数
    val prop = new Properties()
    prop.setProperty("user", "root")
    prop.setProperty("password", "root")
    prop.setProperty("driver", "com.mysql.jdbc.Driver")

    val df = spark.read.jdbc(url, "cardio_data", prop)
    df.show(10)

    val result = df
      .where(col("age") > 0 && col("height").between(50, 250))
      .withColumn("age",round(col("age")/365).cast("int"))
      .withColumn("etl_date", date_format(date_sub(current_timestamp(), 1), "yyyy_MMdd"))

    result.show(10)

    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql("set hive.exec.max.dynamic.partitions=5000")

    // write_to_hive
    result.write.format("hive")
      .mode(SaveMode.Append)
      .partitionBy("etl_date")
      .saveAsTable("hive_yl_ods.cardio_data")

    spark.close()
  }

}

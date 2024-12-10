package com.yl.hive.service1

import com.gy.hive.extract.config.Configurtion
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, current_timestamp, date_format, date_sub}

import java.util.Properties

object lifestyle {

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

    val df = spark.read.jdbc(url, "lifestyle", prop)
    df.show()

    val result = df
      .withColumn("etl_date", date_format(date_sub(current_timestamp(), 1), "yyyyMMdd"))

    // write_to_hive
    result.write.format(Configurtion.HIVE_FORMAT)
      .mode(SaveMode.Append)
      .partitionBy("etldate")
      .saveAsTable("hive_yl_ods.lifestyle")

    spark.close()
  }

}

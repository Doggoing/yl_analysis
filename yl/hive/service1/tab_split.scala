package com.yl.hive.service1

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, current_timestamp, date_format, date_sub, round, shuffle}

import java.util.Properties

object tab_split {
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
    df.show()

    val user_info = df.select("id", "age", "gender", "height", "weight")
      .withColumn("age",round(col("age")/365).cast("int"))
      //.withColumn("etl_date",date_format(date_sub(current_timestamp(),1),"yyyyMMdd"))
      .dropDuplicates()  // 去重

    val health_metrics = df.select("id", "ap_hi", "ap_lo", "cholesterol", "gluc")

    val lifestyle = df.select("id", "smoke", "ap_lo", "alco", "active")

    val cardio_results = df.select("id", "cardio")

//    user_info.write.mode(SaveMode.Append).format("jdbc").jdbc(url,"user_info",prop)
//    health_metrics.write.mode(SaveMode.Append).format("jdbc").jdbc(url,"health_metrics",prop)
//    lifestyle.write.mode(SaveMode.Append).format("jdbc").jdbc(url,"lifestyle",prop)
//    cardio_results.write.mode(SaveMode.Append).format("jdbc").jdbc(url,"cardio_results",prop)

    user_info.show()
    health_metrics.show()
    lifestyle.show()
    cardio_results.show()
    //    spark.sql()
    spark.close()

  }

}

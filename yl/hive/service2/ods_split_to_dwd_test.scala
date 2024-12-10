package com.yl.hive.service2

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{current_timestamp, date_format, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}

object ods_split_to_dwd_test {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]")
      .setAppName("全量加载数据")

    val spark = SparkSession.builder()
      .config(conf)
      //.config("hive.metastore.uris", "thrift://192.168.201.10:9083")
      .enableHiveSupport()
      .getOrCreate()

    val df = spark.read.table("hive_yl_ods.cardio_data$")

    val user_info = df.select("id", "age", "gender", "height", "weight","etl_date")
      .withColumn("dwd_insert_user", lit("user1"))
      .withColumn("dwd_insert_time", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("dwd_modify_user", lit("user1"))
      .withColumn("dwd_modify_time", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
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

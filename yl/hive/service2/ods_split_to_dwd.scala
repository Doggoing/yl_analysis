package com.yl.hive.service1

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, current_timestamp, date_format, date_sub, lit, round, shuffle}

import java.util.Properties

object ods_split_to_dwd {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]")
      .setAppName("全量加载数据")

    val spark = SparkSession.builder()
      .config(conf)
      //.config("hive.metastore.uris", "thrift://192.168.201.10:9083")
      .enableHiveSupport()
      .getOrCreate()

    val df = spark.read.table("hive_yl_ods.cardio_data")

    val insertUser = sys.env.getOrElse("dwd_insert_user", "default_user") // 从环境变量中读取，默认值为 "default_user"

    def addMetadataColumns(df: DataFrame, user: String): DataFrame = {
      df.withColumn("dwd_insert_user", lit(insertUser))
        .withColumn("dwd_insert_time", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("dwd_modify_user", lit(insertUser))
        .withColumn("dwd_modify_time", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
        .dropDuplicates()
    }

    // 应用
    val user_info = addMetadataColumns(df.select("id", "age", "gender", "height", "weight", "etl_date"), insertUser)

    val health_metrics = addMetadataColumns(df.select("id", "ap_hi", "ap_lo", "cholesterol", "gluc", "etl_date"), insertUser)

    val lifestyle = addMetadataColumns(df.select("id", "smoke", "alco", "active", "etl_date"), insertUser)

    val cardio_results = addMetadataColumns(df.select("id", "cardio", "etl_date"), insertUser)

    user_info.show()
    health_metrics.show()
    lifestyle.show()
    cardio_results.show()

    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql("set hive.exec.max.dynamic.partitions=5000")

    user_info.write.mode(SaveMode.Append).format("hive").partitionBy("etl_date").saveAsTable("hive_yl_dwd.dim_user_info")
    health_metrics.write.mode(SaveMode.Append).format("hive").partitionBy("etl_date").saveAsTable("hive_yl_dwd.dim_health_metrics")
    lifestyle.write.mode(SaveMode.Append).format("hive").partitionBy("etl_date").saveAsTable("hive_yl_dwd.dim_lifestyle")
    cardio_results.write.mode(SaveMode.Append).format("hive").partitionBy("etl_date").saveAsTable("hive_yl_dwd.fact_cardio_results")

    //    health_metrics.show()
    //    lifestyle.show()
    //    cardio_results.show()
    //    spark.sql()
    spark.close()

  }

}

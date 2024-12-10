package com.yl.hive.service1

import com.gy.hive.extract.config.Configurtion
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, current_timestamp, date_format, date_sub, round}

import java.util.Properties

// 1. BMI 指标分析表
object ads_bmi_analysis {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]")
      .setAppName("全量加载数据")

    val spark = SparkSession.builder()
      .config(conf)
      //.config("hive.metastore.uris", "thrift://192.168.201.10:9083")
      .enableHiveSupport()
      .getOrCreate()

    val result = spark.sql(
      """
        |SELECT
        |    id,
        |    height,
        |    weight,
        |    etl_date,
        |    ROUND(weight / POW(height / 100, 2), 2) AS bmi, -- 身高转换为米后计算 BMI
        |    CASE
        |        WHEN weight / POW(height / 100, 2) < 18.5 THEN '低体重'
        |        WHEN weight / POW(height / 100, 2) BETWEEN 18.5 AND 24.9 THEN '正常体重'
        |        WHEN weight / POW(height / 100, 2) BETWEEN 25 AND 29.9 THEN '超重'
        |        ELSE '肥胖'
        |    END AS bmi_category
        |FROM
        |    hive_yl_dwd.dim_user_info
        |""".stripMargin)
      .withColumn("bmi",col("bmi").cast("float"))

      result.show()

    import org.apache.spark.sql.functions._

    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql("set hive.exec.max.dynamic.partitions=5000")

    // write_to_hive
    result.write.format("hive")
      .mode(SaveMode.Append)
      .partitionBy("etl_date")
      .saveAsTable("hive_yl_ads.ads_bmi_analysis")

    spark.close()
  }

}

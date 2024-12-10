package com.yl.hive.service1

import com.gy.hive.extract.config.Configurtion
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, current_timestamp, date_format, date_sub, round}

import java.util.Properties

// 3. 血糖风险评估表
object ads_diabetes_risk {

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
        |SELECT id,
        |gluc,
        |etl_date,
        |CASE
        |WHEN gluc = 1 THEN '正常'
        |WHEN gluc = 2 THEN '糖尿病前期'
        |WHEN gluc = 3 THEN '糖尿病'
        |ELSE '未知'
        |END AS diabetes_risk
        |FROM hive_yl_dwd.dim_health_metrics
        |""".stripMargin)

      result.show()

    import org.apache.spark.sql.functions._

    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql("set hive.exec.max.dynamic.partitions=5000")

    // write_to_hive
        result.write.format("hive")
          .mode(SaveMode.Append)
          .partitionBy("etl_date")
          .saveAsTable("hive_yl_ads.ads_diabetes_risk")

    spark.close()
  }

}

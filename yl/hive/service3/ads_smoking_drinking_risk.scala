package com.yl.hive.service1

import com.gy.hive.extract.config.Configurtion
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, current_timestamp, date_format, date_sub, round}

import java.util.Properties

// 4. 吸烟与饮酒风险评估表
object ads_smoking_drinking_risk {

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
        |smoke,
        |alco,
        |etl_date,
        |CASE
        |WHEN smoke = 0 AND alco = 0 THEN '低风险'
        |WHEN smoke = 1 AND alco = 0 THEN '中风险（吸烟）'
        |WHEN smoke = 0 AND alco = 1 THEN '中风险（饮酒）'
        |WHEN smoke = 1 AND alco = 1 THEN '高风险'
        |ELSE '未知'
        |END AS smoking_drinking_risk
        |FROM hive_yl_dwd.dim_lifestyle;
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
          .saveAsTable("hive_yl_ads.ads_smoking_drinking_risk")

    spark.close()
  }

}

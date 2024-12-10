package com.yl.hive.service1

import com.gy.hive.extract.config.Configurtion
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, current_timestamp, date_format, date_sub, round}

import java.util.Properties

// 5. 健康生活方式评分表
object ads_health_risk_analysis {

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
        |    smoke,
        |    alco,
        |    active,
        |    etl_date,
        |    healthy_lifestyle_score,
        |    -- 根据健康生活方式评分映射为生活方式评分标签
        |    CASE
        |        WHEN healthy_lifestyle_score = 3 THEN '非常健康'
        |        WHEN healthy_lifestyle_score = 2 THEN '健康'
        |        WHEN healthy_lifestyle_score = 1 THEN '有风险'
        |        ELSE '不健康'
        |    END AS lifestyle_score_label
        |FROM (
        |    SELECT
        |        id,
        |        smoke,
        |        alco,
        |        active,
        |        etl_date,
        |        -- 计算健康生活方式评分
        |        (CASE
        |            WHEN smoke = 0 THEN 1 ELSE 0 END +
        |         CASE
        |            WHEN alco = 0 THEN 1 ELSE 0 END +
        |         active) AS healthy_lifestyle_score
        |    FROM
        |        hive_yl_dwd.dim_lifestyle
        |) AS lifestyle_scores;
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
          .saveAsTable("hive_yl_ads.ads_health_risk_analysis")

    spark.close()
  }

}

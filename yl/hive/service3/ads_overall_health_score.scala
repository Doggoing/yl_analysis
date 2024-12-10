package com.yl.hive.service1

import com.gy.hive.extract.config.Configurtion
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, current_timestamp, date_format, date_sub, round}

import java.util.Properties

// 6. 综合健康评分表
object ads_overall_health_score {

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
        |    ap_hi,
        |    ap_lo,
        |    gluc,
        |    etl_date,
        |    overall_health_score,
        |    -- 根据 overall_health_score 映射为健康评分标签
        |    CASE
        |        WHEN overall_health_score >= 30 THEN '优秀'
        |        WHEN overall_health_score BETWEEN 20 AND 29 THEN '良好'
        |        WHEN overall_health_score BETWEEN 10 AND 19 THEN '中等'
        |        ELSE '差'
        |    END AS health_score_label
        |FROM (
        |    SELECT
        |        l.id,
        |        l.height,
        |        l.weight,
        |        h.ap_hi,
        |        h.ap_lo,
        |        h.gluc,
        |        h.etl_date,
        |        -- 计算 overall_health_score
        |        (CASE
        |            WHEN (weight / (height * height)) BETWEEN 18.5 AND 24.9 THEN 10 ELSE 0 END +
        |         CASE
        |            WHEN ap_hi < 120 AND ap_lo < 80 THEN 10 ELSE 0 END +
        |         CASE
        |            WHEN gluc = 1 THEN 10 ELSE 0 END) AS overall_health_score
        |    FROM
        |        hive_yl_dwd.dim_user_info l
        |    JOIN
        |        hive_yl_dwd.dim_health_metrics h
        |    ON l.id = h.id
        |) AS health_scores;
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
          .saveAsTable("hive_yl_ads.ads_overall_health_score")

    spark.close()
  }

}

package com.yl.hive.service1

import com.gy.hive.extract.config.Configurtion
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, current_timestamp, date_format, date_sub, round}

import java.util.Properties

// 2. 血压风险评估表
object ads_hypertension_risk {

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
        |ap_hi,
        |ap_lo,
        |etl_date,
        |CASE
        |WHEN ap_hi < 120 AND ap_lo < 80 THEN '正常'
        |WHEN ap_hi BETWEEN 120 AND 129 AND ap_lo < 80 THEN '高血压前期'
        |WHEN ap_hi BETWEEN 130 AND 139 OR ap_lo BETWEEN 80 AND 89 THEN '高血压1级'
        |ELSE '高血压2级'
        |END AS hypertension_risk
        |FROM hive_yl_dwd.dim_health_metrics;
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
          .saveAsTable("hive_yl_ads.ads_hypertension_risk")

    spark.close()
  }

}

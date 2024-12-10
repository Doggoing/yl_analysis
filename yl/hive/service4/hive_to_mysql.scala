package com.yl.hive.service1

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, current_timestamp, date_format, date_sub, lit, round, shuffle}

import java.util.Properties

object hive_to_mysql {
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

    val ads_hypertension_risk = spark.read.table("hive_yl_ads.ads_hypertension_risk")
    val ads_smoking_drinking_risk = spark.read.table("hive_yl_ads.ads_smoking_drinking_risk")
    val ads_overall_health_score = spark.read.table("hive_yl_ads.ads_overall_health_score")
    val ads_health_risk_analysis = spark.read.table("hive_yl_ads.ads_health_risk_analysis")
    val ads_diabetes_risk = spark.read.table("hive_yl_ads.ads_diabetes_risk")
    val ads_bmi_analysis = spark.read.table("hive_yl_ads.ads_bmi_analysis")

    //ads_hypertension_risk.show()
    ads_diabetes_risk.show()

    ads_bmi_analysis.write.format("jdbc").mode(SaveMode.Append).jdbc(url,"heart_test.ads_bmi_analysis",prop)
    //ads_smoking_drinking_risk.write.format("jdbc").mode(SaveMode.Append).jdbc(url,"heart_test.ads_smoking_drinking_risk",prop)
    //ads_overall_health_score.write.format("jdbc").mode(SaveMode.Append).jdbc(url,"heart_test.ads_overall_health_score",prop)
    //ads_health_risk_analysis.write.format("jdbc").mode(SaveMode.Append).jdbc(url,"heart_test.ads_health_risk_analysis",prop)
    ads_diabetes_risk.write.format("jdbc").mode(SaveMode.Append).jdbc(url,"heart_test.ads_diabetes_risk",prop)
    //ads_hypertension_risk.write.format("jdbc").mode(SaveMode.Append).jdbc(url,"heart_test.ads_hypertension_risk",prop)

    spark.close()

  }

}

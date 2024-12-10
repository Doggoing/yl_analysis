package com.yl.hive.service1

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}
import org.apache.spark.sql.functions._

import java.util.Properties

object result {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]")
      .setAppName("全量加载数据")

    val spark = SparkSession.builder()
      .config(conf)
      //.config("hive.metastore.uris", "thrift://192.168.201.10:9083")
      .enableHiveSupport()
      .getOrCreate()

    //数据库连接地址
    val url = "jdbc:mysql://localhost:3306/heart_test?characterEncoding=UTF-8&useSSL=false"
    //数据库连接参数
    val prop = new Properties()
    prop.setProperty("user", "root")
    prop.setProperty("password", "root")
    prop.setProperty("driver", "com.mysql.jdbc.Driver")

    val cardio_results = spark.read.jdbc(url, "cardio_results", prop)
    cardio_results.createTempView("cardio_results")

    val ads_diabetes_risk = spark.read.jdbc(url, "ads_diabetes_risk", prop)
    ads_diabetes_risk.createTempView("ads_diabetes_risk")

    val ads_health_risk_analysis = spark.read.jdbc(url, "ads_health_risk_analysis", prop)
    ads_health_risk_analysis.createTempView("ads_health_risk_analysis")

    val ads_bmi_analysis = spark.read.jdbc(url, "ads_bmi_analysis", prop)
    ads_bmi_analysis.createTempView("ads_bmi_analysis")

    val ads_hypertension_risk = spark.read.jdbc(url, "ads_hypertension_risk", prop)
    ads_hypertension_risk.createTempView("ads_hypertension_risk")

    val ads_overall_health_score = spark.read.jdbc(url, "ads_overall_health_score", prop)
    ads_overall_health_score.createTempView("ads_overall_health_score")

    val ads_smoking_drinking_risk = spark.read.jdbc(url, "ads_smoking_drinking_risk", prop)
    ads_smoking_drinking_risk.createTempView("ads_smoking_drinking_risk")

    val data = spark.sql(
      """
        |select
        |re.id as id,
        |re.cardio,
        |d.diabetes_risk,
        |he.lifestyle_score_label,
        |b.bmi_category ,
        |s.smoking_drinking_risk,
        |o.health_score_label
        |from cardio_results as re
        |join ads_diabetes_risk d on d.id = re.id
        |join ads_health_risk_analysis as he on he.id = re.id
        |join ads_bmi_analysis as b on b.id = re.id
        |join ads_hypertension_risk as hy on hy.id = re.id
        |join ads_smoking_drinking_risk as s on s.id = re.id
        |join ads_overall_health_score as o on o.id = re.id
        |where re.cardio = 1
        |""".stripMargin)
      .sort("id")
      .dropDuplicates()

    // 计算总数
    val totalCount = data.count()

    // 分析生活方式评分与心血管疾病的关系
    val lifestyleDistribution = data.groupBy("lifestyle_score_label")
      .agg(
        functions.count("id").alias("count"),
        avg("cardio").alias("avg_cardio")
      )
      .withColumn("percentage", format_string("%.2f%%", (col("count") / totalCount * 100))) // 转换为百分比格式
      .distinct()
    lifestyleDistribution.show()

    // 吸烟饮酒风险与心血管疾病的关系
    val smokingDrinkingRiskDistribution = data.groupBy("smoking_drinking_risk")
      .agg(
        count("id").alias("count"),
        avg("cardio").alias("avg_cardio")
      )
      .withColumn("percentage", format_string("%.2f%%", (col("count") / totalCount * 100))) // 转换为百分比格式
      .distinct()
    smokingDrinkingRiskDistribution.show()

    // BMI 分类与心血管疾病的关系
    val bmiCategoryDistribution = data.groupBy("bmi_category")
      .agg(
        count("id").alias("count"),
        avg("cardio").alias("avg_cardio")
      )
      .withColumn("percentage", format_string("%.2f%%", (col("count") / totalCount * 100))) // 转换为百分比格式
      .distinct()
    bmiCategoryDistribution.show()

    // 高血压风险与心血管疾病的关系
    val hypertensionRiskDistribution = data.groupBy("health_score_label")
      .agg(
        count("id").alias("count"),
        avg("cardio").alias("avg_cardio")
      )
      .withColumn("percentage", format_string("%.2f%%", (col("count") / totalCount * 100))) // 转换为百分比格式
      .distinct()
    hypertensionRiskDistribution.show()

    // 综合分析，查看多个因素的联合影响
    val combinedRiskAnalysis = data.groupBy("bmi_category", "lifestyle_score_label", "smoking_drinking_risk")
      .agg(
        count("id").alias("count"),
        avg("cardio").alias("avg_cardio")
      )
      .withColumn("percentage", format_string("%.2f%%", (col("count") / totalCount * 100))) // 转换为百分比格式
      .distinct()
    combinedRiskAnalysis.show()

    spark.close()

  }
}

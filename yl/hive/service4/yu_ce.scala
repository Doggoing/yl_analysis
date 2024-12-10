package com.yl.hive.service4

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.classification.{LogisticRegression, RandomForestClassifier}
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.functions._

object yu_ce {

  def main(args: Array[String]): Unit = {

    // 配置 Spark 会话
    val conf = new SparkConf().setMaster("local[*]")
      .setAppName("全量加载数据")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val spark = SparkSession.builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    // 加载数据
    val data = spark.sql(
      """
        SELECT
            b.id,
            b.bmi,
            hsm.overall_health_score,
            CASE
                WHEN h.hypertension_risk = '正常' THEN 0
                WHEN h.hypertension_risk = '高血压前期' THEN 1
                WHEN h.hypertension_risk = '高血压1级' THEN 2
                WHEN h.hypertension_risk = '高血压2级' THEN 3
                ELSE 4
            END AS hypertension_risk,
            l.healthy_lifestyle_score,
            CASE
                WHEN s.smoking_drinking_risk = '低风险' THEN 0
                WHEN s.smoking_drinking_risk = '中风险（饮酒）' THEN 1
                WHEN s.smoking_drinking_risk = '高风险' THEN 2
                ELSE 4
            END AS smoking_drinking_risk,
            CASE
                WHEN hsm.health_score_label = '差' THEN 0
                WHEN hsm.health_score_label = '中等' THEN 1
                WHEN hsm.health_score_label = '良好' THEN 2
                WHEN hsm.health_score_label = '优秀' THEN 3
                ELSE 4
            END AS health_score_label
        FROM hive_yl_ads.ads_bmi_analysis b
        JOIN hive_yl_ads.ads_hypertension_risk h ON b.id = h.id
        JOIN hive_yl_ads.ads_overall_health_score hsm ON b.id = hsm.id
        JOIN hive_yl_ads.ads_health_risk_analysis l ON b.id = l.id
        JOIN hive_yl_ads.ads_smoking_drinking_risk s ON b.id = s.id
      """
    )
      data.show()

    // 数据类型转换
    val dataWithNumbers = data
      .withColumn("bmi", col("bmi").cast("double"))
      .withColumn("overall_health_score", col("overall_health_score").cast("double"))
      .withColumn("hypertension_risk", col("hypertension_risk").cast("double"))
      .withColumn("healthy_lifestyle_score", col("healthy_lifestyle_score").cast("double"))
      .withColumn("smoking_drinking_risk", col("smoking_drinking_risk").cast("double"))
      .withColumn("health_score_label", col("health_score_label").cast("double"))

    // 特征工程：将特征列合并为一个特征向量
    val assembler = new VectorAssembler()
      .setInputCols(Array("bmi", "overall_health_score", "hypertension_risk", "healthy_lifestyle_score", "smoking_drinking_risk"))
      .setOutputCol("features")

    val assembledData = assembler.transform(dataWithNumbers)

    // 训练模型（用所有数据进行训练）
    val lr = new LogisticRegression()
      .setLabelCol("health_score_label")
      .setFeaturesCol("features")
      .setMaxIter(10)
      .setRegParam(0.01)
      .setElasticNetParam(0.8)

    // 训练逻辑回归模型
    val model = lr.fit(assembledData)

    // 用训练数据进行预测
    val predictions = model.transform(assembledData)

    // 评估模型性能（仅适用于训练数据的评估）
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("health_score_label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    // 评估准确度
    val accuracy = evaluator.evaluate(predictions)
    println(s"Train Accuracy = $accuracy")

    import spark.implicits._

    // 对新数据进行预测
    val newData = Seq(
      (28.65, 10, 1, 3, 0), // 示例数据
      (36.0, 10, 2, 3, 0)
    ).toDF("bmi", "overall_health_score", "hypertension_risk", "healthy_lifestyle_score", "smoking_drinking_risk")

    // 特征工程：对新数据进行转换
    val newAssembledData = assembler.transform(newData)

    // 预测新数据
    val newPredictions = model.transform(newAssembledData)

    // 显示预测结果
    newPredictions.select("bmi", "overall_health_score", "prediction").show()

//    // 将字符串字段转换为数值，确保索引操作在主线程中完成
//    val healthScoreIndexer = new StringIndexer()
//      .setInputCol("health_score_label")
//      .setOutputCol("health_score_label_index")
//
//    val hypertensionRiskIndexer = new StringIndexer()
//      .setInputCol("hypertension_risk")
//      .setOutputCol("hypertension_risk_index")
//
//    val smokingDrinkingRiskIndexer = new StringIndexer()
//      .setInputCol("smoking_drinking_risk")
//      .setOutputCol("smoking_drinking_risk_index")
//
//    // 特征组装
//    val assembler = new VectorAssembler()
//      .setInputCols(Array("bmi", "healthy_lifestyle_score", "health_score_label_index", "hypertension_risk_index", "smoking_drinking_risk_index"))
//      .setOutputCol("features")
//
//    // 随机森林回归器
//    val rfClassifier = new RandomForestClassifier()
//      .setLabelCol("overall_health_score")
//      .setFeaturesCol("features")
//      .setPredictionCol("prediction")
//
//    // 构建 Pipeline
//    val pipeline = new Pipeline()
//      .setStages(Array(healthScoreIndexer, hypertensionRiskIndexer, smokingDrinkingRiskIndexer, assembler, rfClassifier))
//
//    // 使用整个数据集训练模型
//    val model = pipeline.fit(data)
//
//    // 模型预测
//    val predictions = model.transform(data)
//
//    // 输出预测结果
//    predictions.show()
//
//    // 关闭 Spark 会话
    spark.close()
  }
}

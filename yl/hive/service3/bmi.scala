//package com.yl.hive.service3
//
//object bmi {
//
//  def main(args: Array[String]): Unit = {
//
//
//
//  }
//
//}
//
////SELECT id,
////height,
////weight,
////(weight / (height * height)) AS bmi,
////CASE
////WHEN (weight / (height * height)) < 18.5 THEN '低体重'
////WHEN (weight / (height * height)) BETWEEN 18.5 AND 24.9 THEN '正常体重'
////WHEN (weight / (height * height)) BETWEEN 25 AND 29.9 THEN '超重'
////ELSE '肥胖'
////END AS bmi_category
////FROM ads_health_risk_analysis;
//
//SELECT id,
//ap_hi,
//ap_lo,
//CASE
//WHEN ap_hi < 120 AND ap_lo < 80 THEN '正常'
//WHEN ap_hi BETWEEN 120 AND 129 AND ap_lo < 80 THEN '高血压前期'
//WHEN ap_hi BETWEEN 130 AND 139 OR ap_lo BETWEEN 80 AND 89 THEN '高血压1级'
//ELSE '高血压2级'
//END AS hypertension_risk
//FROM ads_health_risk_analysis;
//
//SELECT id,
//gluc,
//CASE
//WHEN gluc = 1 THEN '正常'
//WHEN gluc = 2 THEN '糖尿病前期'
//WHEN gluc = 3 THEN '糖尿病'
//ELSE '未知'
//END AS diabetes_risk
//FROM ads_health_risk_analysis;
//
//SELECT id,
//smoke,
//alco,
//CASE
//WHEN smoke = 0 AND alco = 0 THEN '低风险'
//WHEN smoke = 1 AND alco = 0 THEN '中风险（吸烟）'
//WHEN smoke = 0 AND alco = 1 THEN '中风险（饮酒）'
//WHEN smoke = 1 AND alco = 1 THEN '高风险'
//ELSE '未知'
//END AS smoking_drinking_risk
//FROM hive_yl_dwd.dim_lifestyle;
//
//SELECT id,
//smoke,
//alco,
//active,
//(CASE WHEN smoke = 0 THEN 1 ELSE 0 END +
//  CASE WHEN alco = 0 THEN 1 ELSE 0 END +
//  active) AS healthy_lifestyle_score
//FROM ads_health_risk_analysis;
//
//SELECT id,
//height,
//weight,
//ap_hi,
//ap_lo,
//gluc,
//(CASE WHEN (weight / (height * height)) BETWEEN 18.5 AND 24.9 THEN 10 ELSE 0 END +
//  CASE WHEN ap_hi < 120 AND ap_lo < 80 THEN 10 ELSE 0 END +
//  CASE WHEN gluc = 1 THEN 10 ELSE 0 END) AS overall_health_score
//FROM ads_health_risk_analysis;
//

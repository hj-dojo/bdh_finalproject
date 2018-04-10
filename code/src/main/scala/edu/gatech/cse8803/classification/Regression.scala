/**
  * @Author Hazel John on 4/1/18.
  */
package edu.gatech.cse8803.classification

import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Regression {

  def runLogisticRegression(featureDF: DataFrame) =
  {
    val ss = featureDF.sparkSession
    import ss.implicits._

    /** scale features */
    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .setWithStd(true)
      .setWithMean(false)

    // Compute summary statistics by fitting the StandardScaler.
    val scalerModel = scaler.fit(featureDF)

    // Normalize each feature to have unit standard deviation.
    val scaledData = scalerModel.transform(featureDF)

    val lr = new LogisticRegression()
      .setFeaturesCol("scaledFeatures")
      .setLabelCol("label")
      .setMaxIter(50)
      .setRegParam(0.01)
      .setElasticNetParam(0.8)
      .setTol(0.05)

    // Fit the model
    val lrModel = lr.fit(scaledData)

    /* Report some metrics */
    reportModelMetrics(featureDF.sparkSession, lrModel, false)

    // We can also use the multinomial family for binary classification
    val mlr = new LogisticRegression()
      .setFeaturesCol("scaledFeatures")
      .setLabelCol("label")
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
      .setTol(0.05)
      .setFamily("multinomial")

    val mlrModel = mlr.fit(scaledData)

    /* Report some metrics */
    reportModelMetrics(featureDF.sparkSession, mlrModel, true)
  }

  def reportModelMetrics(ss: SparkSession, lrModel: LogisticRegressionModel, isMulti: Boolean) = {

    import ss.implicits._

    // Print the coefficients and intercept for logistic regression
    if (isMulti) {
      // Print the coefficients and intercepts for logistic regression with multinomial family
      println(s"Multinomial coefficients: ${lrModel.coefficientMatrix}")
      println(s"Multinomial intercepts: ${lrModel.interceptVector}")
    }
    else
    {
      println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")
    }

    val trainingSummary = lrModel.binarySummary

    // Obtain the objective per iteration.
    val objectiveHistory = trainingSummary.objectiveHistory
    println("objectiveHistory:")
    objectiveHistory.foreach(loss => println(loss))

    // Obtain the receiver-operating characteristic as a dataframe and areaUnderROC.
    val roc = trainingSummary.roc
    roc.show()
    println(s"areaUnderROC: ${trainingSummary.areaUnderROC}")

    // Set the model threshold to maximize F-Measure
    val fMeasure = trainingSummary.fMeasureByThreshold
    val maxFMeasure = fMeasure.select(max("F-Measure")).head().getDouble(0)
    val bestThreshold = fMeasure.where($"F-Measure" === maxFMeasure)
                                .select("threshold").head().getDouble(0)

    lrModel.setThreshold(bestThreshold)
  }
}

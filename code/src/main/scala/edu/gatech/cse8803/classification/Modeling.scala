/**
  * @Author Hazel John on 4/1/18.
  */
package edu.gatech.cse8803.classification

import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel, RandomForestClassifier}
import org.apache.spark.ml.feature.{LabeledPoint, MinMaxScaler, StandardScaler}
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Modeling {

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
      .setMaxIter(50)
      .setRegParam(0.001)
      .setElasticNetParam(1.0)
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
//    // Obtain the objective per iteration.
//    val objectiveHistory = trainingSummary.objectiveHistory
//    println("objectiveHistory:")
//    objectiveHistory.foreach(loss => println(loss))

    // Obtain the receiver-operating characteristic as a dataframe and areaUnderROC.
    val roc = trainingSummary.roc
    roc.show()
    println(s"areaUnderROC: ${trainingSummary.areaUnderROC}")

    // Set the model threshold to maximize F-Measure
    val fMeasure = trainingSummary.fMeasureByThreshold
    val maxFMeasure = fMeasure.select(max("F-Measure")).head().getDouble(0)
    val bestThreshold = fMeasure.where($"F-Measure" === maxFMeasure)
                                .select("threshold").head().getDouble(0)

    println(s"Best Threshold: ${bestThreshold}")

    lrModel.setThreshold(bestThreshold)
  }

  def runLogisticRegressionwithValidation(featureDF: DataFrame) =
  {
    val ss = featureDF.sparkSession
    import ss.implicits._

    // Split data into training (60%) and test (40%)
    val Array(training, test) = featureDF.randomSplit(Array(0.6, 0.4), seed = 11L)
    training.cache()

    /** scale features */
    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .setWithStd(true)
      .setWithMean(false)

    // Compute summary statistics by fitting the StandardScaler.
    val scalerModel = scaler.fit(training)

    // Normalize each feature to have unit standard deviation.
    val scaledTraining = scalerModel.transform(training)
    val scaledTest = scalerModel.transform(test)

    // Run training algorithm to build the model
    val lr = new LogisticRegression()
      .setFeaturesCol("scaledFeatures")
      .setLabelCol("label")
      .setMaxIter(50)

    // We use a ParamGridBuilder to construct a grid of parameters to search over.
    // TrainValidationSplit will try all combinations of values and determine best model using
    // the evaluator.
    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.1, 0.01))
      .addGrid(lr.fitIntercept)
      .addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0))
      .addGrid(lr.tol, Array(0.01, 0.05))
      .build()

    // Define evaluator object
    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setRawPredictionCol("rawPrediction")
      .setMetricName("areaUnderROC") // areaUnderROC (default) or areaUnderPR"

    // Setup Cross validator with estimator, evaluator and param grid
    val cv = new CrossValidator()
      .setEstimator(lr)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)  // Use 3+ in practice
      .setParallelism(2)  // Evaluate up to 2 parameter settings in parallel

    // Run cross-validation, and choose the best set of parameters.
    val cvModel = cv.fit(scaledTraining)

    println(cvModel.bestModel.extractParamMap())

    // Validate with the test
    val predictions = cvModel.transform(scaledTest)

    // Extract relevant metrics
    println("areaUnderROC: " + evaluator.setMetricName("areaUnderROC").evaluate(predictions))
    println("areaUnderPR: " + evaluator.setMetricName("areaUnderPR").evaluate(predictions))

    // Define evaluator object
    val evaluator2 = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")

    println("F1-Score: " + evaluator2.setMetricName("f1").evaluate(predictions))
    println("Weighted Precision: " + evaluator2.setMetricName("weightedPrecision").evaluate(predictions))
    println("weighted Recall: " + evaluator2.setMetricName("weightedRecall").evaluate(predictions))
    println("Accuracy : " + evaluator2.setMetricName("accuracy").evaluate(predictions))
  }

  def runRandomForestwithValidation(featureDF: DataFrame) =
  {
    val ss = featureDF.sparkSession
    import ss.implicits._

    // Split data into training (60%) and test (40%)
    val Array(training, test) = featureDF.randomSplit(Array(0.6, 0.4), seed = 11L)
    training.cache()

    /** scale features */
    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .setWithStd(true)
      .setWithMean(false)

    // Compute summary statistics by fitting the StandardScaler.
    val scalerModel = scaler.fit(training)

    // Normalize each feature to have unit standard deviation.
    val scaledTraining = scalerModel.transform(training)
    val scaledTest = scalerModel.transform(test)

    // Run training algorithm to build the model
    val rf = new RandomForestClassifier()
      .setFeaturesCol("scaledFeatures")
      .setLabelCol("label")
      .setSeed(207336481L)

    // We use a ParamGridBuilder to construct a grid of parameters to search over.
    // TrainValidationSplit will try all combinations of values and determine best model using
    // the evaluator.
    val paramGrid = new ParamGridBuilder()
      .addGrid(rf.maxBins, Array(25, 31, 49))
      .addGrid(rf.impurity, Array("gini", "entropy"))
      .addGrid(rf.maxDepth, Array(4, 6, 8))
      .addGrid(rf.numTrees, Array(20, 50, 80))
      .build()

    // Define evaluator object
    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setRawPredictionCol("rawPrediction")
      .setMetricName("areaUnderROC") // areaUnderROC (default) or areaUnderPR"

    // Setup Cross validator with estimator, evaluator and param grid
    val cv = new CrossValidator()
      .setEstimator(rf)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)  // Use 3+ in practice
      .setParallelism(2)  // Evaluate up to 2 parameter settings in parallel

    // Run cross-validation, and choose the best set of parameters.
    val cvModel = cv.fit(scaledTraining)

    println(cvModel.bestModel.extractParamMap())

    // Fit the test
    val predictions = cvModel.transform(scaledTest)

    // Extract relevant metrics
    println("areaUnderROC: " + evaluator.setMetricName("areaUnderROC").evaluate(predictions))
    println("areaUnderPR: " + evaluator.setMetricName("areaUnderPR").evaluate(predictions))

    // Define evaluator object
    val evaluator2 = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")

    println("F1-Score: " + evaluator2.setMetricName("f1").evaluate(predictions))
    println("Weighted Precision: " + evaluator2.setMetricName("weightedPrecision").evaluate(predictions))
    println("weighted Recall: " + evaluator2.setMetricName("weightedRecall").evaluate(predictions))
    println("Accuracy : " + evaluator2.setMetricName("accuracy").evaluate(predictions))

  }
}

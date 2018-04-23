package bdhproject.sepsis.classification

import org.apache.spark.ml.classification._
import org.apache.spark.ml.feature.{LabeledPoint, MinMaxScaler, StandardScaler}
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics

import scala.collection.mutable.ArrayBuffer

object Modeling {

  val TRAIN_PCT = 0.67

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

  /**
    * Report metrics and model details
    * @param ss
    * @param lrModel
    * @param isMulti
    * @return
    */
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

  /**
    * Evaluate classification metrics given the predictions
    * @param predictions
    */
  def getBinaryClassificationMetrics(modelType:String, predictions: DataFrame) = {

    /* Retrieve predictions and labels */
    val  rawpredictionAndLabels = predictions.select("rawPrediction", "label")
      .rdd.map(x => (x(0).asInstanceOf[DenseVector](1), x(1).asInstanceOf[Double]))

    /* Evaluate metrics */
    val metrics = new BinaryClassificationMetrics(rawpredictionAndLabels)

//    // Precision by threshold
//    val precision = metrics.precisionByThreshold
//    precision.foreach { case (t, p) =>
//      println(s"Threshold: $t, Precision: $p")
//    }
//
//    // Recall by threshold
//    val recall = metrics.recallByThreshold
//    recall.foreach { case (t, r) =>
//      println(s"Threshold: $t, Recall: $r")
//    }
//
//    // Precision-Recall Curve
//    val PRC = metrics.pr
//
//    //Compute thresholds used in ROC and PR curves
//    val thresholds = precision.map(_._1)
//
//    // ROC Curve
//    val roc = metrics.roc
//    // F-measure
//    val f1Score = metrics.fMeasureByThreshold
//    f1Score.foreach { case (t, f) =>
//      println(s"Threshold: $t, F-score: $f, Beta = 1")
//    }
//
//    val beta = 0.5
//    val fScore = metrics.fMeasureByThreshold(beta)
//    f1Score.foreach { case (t, f) =>
//      println(s"Threshold: $t, F-score: $f, Beta = 0.5")
//    }

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")

    // AUPRC
    val auPRC = metrics.areaUnderPR
    // AUROC
    val auROC = metrics.areaUnderROC

    /** Save the metrics */
    /** Commented out metrics from MulticlassClassificationEvaluator as they don't seem correct */
    val modelMetrics = ArrayBuffer(
      Row(modelType, "Accuracy", evaluator.setMetricName("accuracy").evaluate(predictions)),
      Row(modelType, "F1", evaluator.setMetricName("f1").evaluate(predictions)),
      Row(modelType, "weightedPrecision", evaluator.setMetricName("weightedPrecision").evaluate(predictions)),
      Row(modelType, "weightedRecall", evaluator.setMetricName("weightedRecall").evaluate(predictions)),
      Row(modelType, "AUROC", auROC),
      Row(modelType, "AUPRC", auPRC))

    modelMetrics
  }

  /**
    * Run Logistic regression using train/test split for validation and
    * crossvalidation for parameter selection
    * @param featureDF
    */
  def runLogisticRegressionwithValidation(featureDF: DataFrame) =
  {
    val ss = featureDF.sparkSession
    import ss.implicits._

    // Split data into training (60%) and test (40%)
    val Array(training, test) = featureDF.randomSplit(Array(TRAIN_PCT, (1-TRAIN_PCT)), seed = 11L)
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
      .addGrid(lr.elasticNetParam, Array(0.0, 0.5))
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

    val lrModel = cvModel.bestModel.asInstanceOf[LogisticRegressionModel]
    println(s"LR Model coefficients:\n${lrModel.coefficients.toArray.mkString("\n")}")

    println("Best Params (Binomial Logistic Regression): ")
    println(cvModel.bestModel.extractParamMap())

    training.unpersist(false)

    // Validate with the test
    val predictions = cvModel.transform(scaledTest)

    /* Retrieve metrics */
    getBinaryClassificationMetrics("LogisticRegression", predictions)
  }

  /**
    * Run RandomForest classifier using train/test split for validation and
    * crossvalidation for parameter selection
    * @param featureDF
    */
  def runRandomForestwithValidation(featureDF: DataFrame) =
  {
    val ss = featureDF.sparkSession
    import ss.implicits._

    // Split data into training (60%) and test (40%)
    val Array(training, test) = featureDF.randomSplit(Array(TRAIN_PCT, (1-TRAIN_PCT)), seed = 11L)
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

    println("Best Params (RandomForestClassifier): ")
    println(cvModel.bestModel.extractParamMap())

    training.unpersist(false)

    /* Fit the test */
    val predictions = cvModel.transform(scaledTest)

    /* Retrieve metrics */
    getBinaryClassificationMetrics("RandomForest", predictions)
  }

  /**
    * Run GradientBoostedTrees classifier using train/test split for validation and
    * crossvalidation for parameter selection
    * @param featureDF
    */
  def runGradientBoostedTreeswithValidation(featureDF: DataFrame) =
  {
    val ss = featureDF.sparkSession
    import ss.implicits._

    // Split data into training (60%) and test (40%)
    val Array(training, test) = featureDF.randomSplit(Array(TRAIN_PCT, (1-TRAIN_PCT)), seed = 11L)
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
    // Train a GBT model.
    val gbt = new GBTClassifier()
      .setLabelCol("label")
      .setFeaturesCol("scaledFeatures")
      .setMaxIter(50)
      .setSeed(207336481L)

    // We use a ParamGridBuilder to construct a grid of parameters to search over.
    // TrainValidationSplit will try all combinations of values and determine best model using
    // the evaluator.
    val paramGrid = new ParamGridBuilder()
      .addGrid(gbt.maxBins, Array(23, 31))
      .addGrid(gbt.maxDepth, Array(3, 5))
      .build()

    // Define evaluator object
    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setRawPredictionCol("rawPrediction")
      .setMetricName("areaUnderROC") // areaUnderROC (default) or areaUnderPR"

    // Setup Cross validator with estimator, evaluator and param grid
    val cv = new CrossValidator()
      .setEstimator(gbt)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)  // Use 3+ in practice
      .setParallelism(2)  // Evaluate up to 2 parameter settings in parallel

    // Run cross-validation, and choose the best set of parameters.
    val cvModel = cv.fit(scaledTraining)

    println("Best Params (Gradient Boosted Trees): ")
    println(cvModel.bestModel.extractParamMap())

    /* Fit the test */
    val predictions = cvModel.transform(scaledTest)

    training.unpersist(false)

    /* Retrieve metrics */
    getBinaryClassificationMetrics("GradientBoostedTrees", predictions)
  }

  /**
    * Run  MultiLayerPerceptronclassifier using train/test split for validation and
    * crossvalidation for parameter selection
    * @param featureDF
    */
  def runMultiLayerPerceptronwithValidation(featureDF: DataFrame) =
  {
    val ss = featureDF.sparkSession
    import ss.implicits._

    // Split data into training (60%) and test (40%)
    val Array(training, test) = featureDF.randomSplit(Array(TRAIN_PCT, (1-TRAIN_PCT)), seed = 11L)
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

    // specify layers for the neural network:
    // input layer of size 12 (features), two intermediate of size 8 and 5
    // and output of size 2 (classes)
    val layers = Array[Int](12, 8, 5, 2)

    // Run training algorithm to build the model
    // Train a multilayerperceptron model.
    // create the trainer and set its parameters
    val mpc = new MultilayerPerceptronClassifier()
      .setLayers(layers)
      .setBlockSize(128)
      .setSeed(207336481L)
      .setMaxIter(100)

    // We use a ParamGridBuilder to construct a grid of parameters to search over.
    // TrainValidationSplit will try all combinations of values and determine best model using
    // the evaluator.
    val paramGrid = new ParamGridBuilder()
      .addGrid(mpc.tol, Array(0.001, 0.01))
      .build()

    // Define evaluator object
    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setRawPredictionCol("rawPrediction")
      .setMetricName("areaUnderROC") // areaUnderROC (default) or areaUnderPR"

    // Setup Cross validator with estimator, evaluator and param grid
    val cv = new CrossValidator()
      .setEstimator(mpc)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)  // Use 3+ in practice
      .setParallelism(2)  // Evaluate up to 2 parameter settings in parallel

    // Run cross-validation, and choose the best set of parameters.
    val cvModel = cv.fit(scaledTraining)

    println("Best Params (MultilayerPerceptron): ")
    println(cvModel.bestModel.extractParamMap())

    /* Fit the test */
    val predictions = cvModel.transform(scaledTest)

    training.unpersist(false)

    /* Report metrics */
    getBinaryClassificationMetrics("MultiLayerPerceptron", predictions)
  }
}

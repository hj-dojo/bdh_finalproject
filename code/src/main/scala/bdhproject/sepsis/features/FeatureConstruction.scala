/**
  * @Author Hazel John on 4/1/18.
  */
package bdhproject.sepsis.features

import java.sql.Timestamp

import bdhproject.sepsis.model.{ChartEvents, ICUStay}
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD

object FeatureConstruction {

  /**
   * ((icustayid, vitals_itemid), vitals-value)
   */
  type FeatureTuple = ((Int, Int), Double)

  val GCS_VERBAL = 223900
  val GCS_MOTOR = 223901
  val GCS_EYE = 220739
  val SYS_BP_NONINV = 220179
  val DIA_BP_NONINV = 220180
  val HEART_RATE = 220045
  val RESP_RATE = 220210
  val TEMP_F = 223761
  val O2_SAT = 220277

  def qSofaScore(vitals: Array[(Int, Double)]): Double = {

    var qSofa :Double = 0.0

    for ((id, value) <- vitals) {
      val score = id match {
        case GCS_VERBAL if value < 15 => 1
        case SYS_BP_NONINV  if value <= 100 => 1
        case RESP_RATE if value >= 22  => 1
        case _  => 0
      }
      qSofa += score
    }
    qSofa
  }
  /**
    * Retrieve latest feature tuples from chartevents and ICUStays (patient demographic)
    * @param indexdates RDD giving index dates for icustays
    * @param chartevents RDD of chart events of vitals
    * @param icustays RDD of icustays
    * @param predWindowDurHrs Size of prediction window in hours
    * @param obsWindowDurHrs Size of observation window in hours
    * @return RDD of feature tuples
   */
  def constructFeaturesWithLatestEvents(ss: SparkSession, saveDir: String, indexdates: RDD[(Int, Timestamp, Int)],
                                        chartevents: RDD[ChartEvents], icustays: RDD[ICUStay],
                                        predWindowDurHrs: Int, obsWindowDurHrs: Int):  DataFrame = {
    import ss.implicits._

    /** Create UDFs to support age and gender conversions
      * MIMICIII obfuscates ages for patients with age > 89 by setting it to 3000
      * We can either filter those patients out, or set to a static number (eg 90.0) */

    ss.sqlContext.udf.register("getGenderCode", (gender: String) => (if (gender == "F") 0.0 else 1.0))
    ss.sqlContext.udf.register("adjustAge", (age: Double) => (if (age == 300) 90.0 else age))

    val chartevents_df = chartevents.toDF()
    chartevents_df.createOrReplaceTempView("CHARTEVENTS")
//    chartevents_df.printSchema()

    val indexdates_df = indexdates.toDF("hadmID", "indexDate", "label")
    indexdates_df.createOrReplaceTempView("INDEXDATES")
//    indexdates_df.printSchema()

    val icustay_df = icustays.toDF()
    icustay_df.createOrReplaceTempView("ICUSTAYS")

    val pred_window_dur = predWindowDurHrs * 3600
    val obs_window_dur = obsWindowDurHrs * 3600

    /** Extract latest events in the observation window */
    val chartevents_filtered = ss.sql("SELECT CHARTEVENTS.hadmID as hadmID, label, itemID, chartTime, value " +
      "FROM CHARTEVENTS INNER JOIN INDEXDATES " +
      "ON CHARTEVENTS.hadmID = INDEXDATES.hadmID " +
      "WHERE (unix_timestamp(indexdate) - unix_timestamp(chartTime)) BETWEEN " + pred_window_dur + " AND " + obs_window_dur)

//    println("filtered count: " + chartevents_filtered.count())
//    chartevents_filtered.take(5).foreach(println)

    /* Retrieve the latest observation */
    val w = Window.partitionBy($"hadmID", $"label", $"itemID").orderBy($"chartTime".desc)
    val chart_features = chartevents_filtered.withColumn("rn", row_number.over(w)).where($"rn" === 1)
                                               .drop("rn", "chartTime")

    chart_features.createOrReplaceTempView("CHART_FEATURES")
//    println("chart_features  count: " + chart_features.count())
//    chart_features.printSchema()

    /* Calculate qSofa score */
    val qsofa_features = chart_features
      .map(x => ((x.getInt(0), x.getInt(1)), (x.getInt(2), x.getDouble(3))))
      .rdd
      .groupByKey()
      .map(x => (x._1._1, x._1._2, 1, qSofaScore(x._2.toArray)))
      .toDF("hadmID", "label", "itemID", "value")

//    qsofa_features.printSchema()
//    println("qsofa_features count " + qsofa_features.count())
//    val large_qsofa = qsofa_features.filter(x => x.getDouble(3) >= 2)
//    println("large qSofa  count: " + large_qsofa.count())
//    val small_qsofa = qsofa_features.filter(x => x.getDouble(3) < 2)
//    println("small qSofa  count: " + small_qsofa.count())

    val demographic_features = ss.sql("SELECT ICUSTAYS.hadmID as hadmID, label, " +
      "adjustAge(age) as age, getGenderCode(gender) as gender " +
      "FROM CHART_FEATURES INNER JOIN ICUSTAYS " +
      "ON CHART_FEATURES.hadmID = ICUSTAYS.hadmID ").dropDuplicates(Seq("hadmID", "label"))
      .flatMap(x => for (i <- 2 to 3) yield (x.getInt(0), x.getInt(1), i,  x.getDouble(i)) )
      .toDF("hadmID", "label", "itemID", "value")

//    println("demographic_features  count: " + demographic_features.count())
//    demographic_features.take(5).foreach(println)

    val features = chart_features.union(demographic_features).union(qsofa_features)
//    println("features  count: " + features.count())

    /** create a item_id to zero based feature id map */
    val feature_map = features.map(x => x.getInt(2)).rdd.distinct().sortBy(x => x).collect.zipWithIndex.toMap
    val feature_length = feature_map.size
//    println("Feature length: " + feature_length)
//    feature_map.take(10).foreach(println)

    /** Extract the label and feature vectors */
    val featuresDF= features
      .map(x => ((x.getInt(0), x.getInt(1)), (feature_map(x.getInt(2)), x.getDouble(3))))
      .rdd.groupByKey().map(x => LabeledPoint(x._1._2.toDouble, Vectors.sparse(feature_length, x._2.toArray)))
      .toDF("label","features").coalesce(1).cache()

//    println("FEATURES")
//    featuresDF.take(5).foreach(println)
//    featuresDF.printSchema()

    featuresDF.write.mode(SaveMode.Overwrite).format("libsvm").save(saveDir+"/svmoutput/features" + predWindowDurHrs.toString + "hour")

    featuresDF
  }

  /**
    * Retrieve aggregate feature tuples from chartevents and ICUStays (patient demographic)
    * @param indexdates RDD giving index dates for icustays
    * @param chartevents RDD of chart events of vitals
    * @param icustays RDD of icustays
    * @param predWindowDurHrs Size of prediction window in hours
    * @param obsWindowDurHrs Size of observation window in hours
    * @return RDD of feature tuples
    */
  def constructFeaturesWithAggregateEvents(ss: SparkSession, saveDir: String, indexdates: RDD[(Int, Timestamp, Int)],
                                        chartevents: RDD[ChartEvents], icustays: RDD[ICUStay],
                                        predWindowDurHrs: Int, obsWindowDurHrs: Int):  DataFrame = {
    import ss.implicits._

    /** Create UDFs to support age and gender conversions */

    ss.sqlContext.udf.register("getGenderCode", (gender: String) => (if (gender == "F") 0.0 else 1.0))
    ss.sqlContext.udf.register("adjustAge", (age: Double) => (if (age == 300) 90.0 else age))

    val chartevents_df = chartevents.toDF()
    chartevents_df.createOrReplaceTempView("CHARTEVENTS")
    //    chartevents_df.printSchema()

    val indexdates_df = indexdates.toDF("hadmID", "indexDate", "label")
    indexdates_df.createOrReplaceTempView("INDEXDATES")
    //    indexdates_df.printSchema()

    val icustay_df = icustays.toDF()
    icustay_df.createOrReplaceTempView("ICUSTAYS")

    val pred_window_dur = predWindowDurHrs * 3600
    val obs_window_dur = obsWindowDurHrs * 3600

    /** Extract events in the observation window */
    val chartevents_filtered = ss.sql("SELECT CHARTEVENTS.hadmID as hadmID, label, itemID, value " +
      "FROM CHARTEVENTS INNER JOIN INDEXDATES " +
      "ON CHARTEVENTS.hadmID = INDEXDATES.hadmID " +
      "WHERE (unix_timestamp(indexdate) - unix_timestamp(chartTime)) BETWEEN " + pred_window_dur + " AND " + obs_window_dur)

//    println("filtered count: " + chartevents_filtered.count())
//    chartevents_filtered.take(5).foreach(println)

    /* Retrieve the aggregate observations */
    val exprs = Map("value" -> "avg")
    val chart_features = chartevents_filtered.groupBy($"hadmID", $"label", $"itemID")
                                             //.mean("value")
                                             .agg(exprs)
                                             .toDF("hadmID", "label", "itemID", "value")

    chart_features.createOrReplaceTempView("CHART_FEATURES")

//    println("chart_features  count: " + chart_features.count())
//    chart_features.printSchema()

    /* Calculate qSofa score */
    val qsofa_features = chart_features
      .map(x => ((x.getInt(0), x.getInt(1)), (x.getInt(2), x.getDouble(3))))
      .rdd
      .groupByKey()
      .map(x => (x._1._1, x._1._2, 1, qSofaScore(x._2.toArray)))
      .toDF("hadmID", "label", "itemID", "value")

    val demographic_features = ss.sql("SELECT ICUSTAYS.hadmID as hadmID, label, " +
      "adjustAge(age) as age, getGenderCode(gender) as gender " +
      "FROM CHART_FEATURES INNER JOIN ICUSTAYS " +
      "ON CHART_FEATURES.hadmID = ICUSTAYS.hadmID ").dropDuplicates(Seq("hadmID", "label"))
      .flatMap(x => for (i <- 2 to 3) yield (x.getInt(0), x.getInt(1), i, x.getDouble(i)) )
      .toDF("hadmID", "label", "itemID", "value")

//    println("demographic_features  count: " + demographic_features.count())
//    demographic_features.take(5).foreach(println)
//    demographic_features.printSchema()

    val features = chart_features.union(demographic_features).union(qsofa_features)
//    println("features  count: " + features.count())

    /** create a item_id to zero based feature id map */
    val feature_map = features.map(x => x.getInt(2)).rdd.distinct().sortBy(x => x).collect.zipWithIndex.toMap
    val feature_length = feature_map.size
//    println("Feature length: " + feature_length)
//    feature_map.take(10).foreach(println)

    /** Extract the label and feature vectors */
    val featuresDF= features
      .map(x => ((x.getInt(0), x.getInt(1)), (feature_map(x.getInt(2)), x.getDouble(3))))
      .rdd.groupByKey().map(x => LabeledPoint(x._1._2.toDouble, Vectors.sparse(feature_length, x._2.toArray)))
      .toDF("label","features").coalesce(1).cache()

//    println("FEATURES")
//    featuresDF.take(5).foreach(println)
//    featuresDF.printSchema()

    featuresDF.write.mode(SaveMode.Overwrite).format("libsvm").save(saveDir+"/svmoutput/aggfeatures" + predWindowDurHrs.toString + "hour")

    featuresDF
  }
}



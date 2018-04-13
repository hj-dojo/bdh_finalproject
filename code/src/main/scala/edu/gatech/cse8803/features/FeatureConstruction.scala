/**
  * @Author Hazel John on 4/1/18.
  */
package edu.gatech.cse8803.features

import java.sql.Timestamp

import edu.gatech.cse8803.model.ChartEvents
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD

object FeatureConstruction {

  /* Define length of prediction window in seconds */
  val PREDICTION_WINDOW_S = 4 * 60 * 60
  /**
   * ((icustayid, vitals_itemid), vitals-value)
   */
  type FeatureTuple = ((Int, Int), Double)

  /**
    * Aggregate feature tuples from chartevents
    * @param indexdates RDD giving index dates for icustays
    * @param chartevents RDD of chart events of vitals
    * @return RDD of feature tuples
   */
  def constructFeaturesWithLatestEvents(ss: SparkSession, saveDir: String, indexdates: RDD[(Int, Timestamp, Int)],
                                       chartevents: RDD[ChartEvents], predWindowDurHrs: Int):  DataFrame = {
    import ss.implicits._


    val chartevents_df = chartevents.toDF()
    chartevents_df.createOrReplaceTempView("CHARTEVENTS")
//    chartevents_df.printSchema()

    val indexdates_df = indexdates.toDF("hadmID", "indexDate", "label")
    indexdates_df.createOrReplaceTempView("INDEXDATES")
//    indexdates_df.printSchema()

    val pred_window_dur = predWindowDurHrs * 3600

    /** Extract events in the observation window */
    val chartevents_filtered = ss.sql("SELECT CHARTEVENTS.hadmID as hadmID, label, itemID, chartTime, value " +
      "FROM CHARTEVENTS INNER JOIN INDEXDATES " +
      "ON CHARTEVENTS.hadmID = INDEXDATES.hadmID " +
      "WHERE (unix_timestamp(indexdate) - unix_timestamp(chartTime)) > " + pred_window_dur)

//    println("filtered count: " + chartevents_filtered.count())

    /* Retrieve the latest observation */
    val w = Window.partitionBy($"hadmID", $"label", $"itemID").orderBy($"chartTime".desc)
    val features = chartevents_filtered.withColumn("rn", row_number.over(w)).where($"rn" === 1).drop($"rn")

//    println("filtered single items per type count: " + features.count())
//    features.take(10).foreach(println)

    /** create a item_id to zero based feature id map */
    val feature_map = features.map(x => x.getInt(2)).rdd.distinct().sortBy(x => x).collect.zipWithIndex.toMap
    val feature_length = feature_map.size
//    println("Feature length: " + feature_length)
//    feature_map.take(10).foreach(println)

    /** Extract the label and feature vectors */
    val featuresDF= features
      .map(x => ((x.getInt(0), x.getInt(1)), (feature_map(x.getInt(2)), x.getDouble(4))))
      .rdd.groupByKey().map(x => LabeledPoint(x._1._2.toDouble, Vectors.sparse(feature_length, x._2.toArray)))
      .toDF("label","features").coalesce(1)

      println("FEATURES")

       featuresDF.take(5).foreach(println)
       featuresDF.printSchema()

    featuresDF.write.mode(SaveMode.Overwrite).format("libsvm").save(saveDir+"/svmoutput/features" + predWindowDurHrs.toString + "hour")

    featuresDF
  }
}



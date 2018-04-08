/**
  * @Author Hazel John on 4/1/18.
  */
package edu.gatech.cse8803.features

import java.sql.Timestamp

import edu.gatech.cse8803.model.ChartEvents
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}
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
  def constructChartEventsFeatureTuple(ss: SparkSession, saveDir: String, indexdates: RDD[(Int, Timestamp, Int)],
                                       chartevents: RDD[ChartEvents]):  RDD[FeatureTuple] = {
    import ss.implicits._


    val chartevents_df = chartevents.toDF()
    chartevents_df.createOrReplaceTempView("CHARTEVENTS")
    chartevents_df.printSchema()

    val indexdates_df = indexdates.toDF("hadmID", "indexDate", "label")
    indexdates_df.createOrReplaceTempView("INDEXDATES")
    indexdates_df.printSchema()

    val chartevents_filtered = ss.sql("SELECT CHARTEVENTS.hadmID as hadmID, label, itemID, chartTime, value " +
      "FROM CHARTEVENTS INNER JOIN INDEXDATES " +
      "ON CHARTEVENTS.hadmID = INDEXDATES.hadmID " +
      "WHERE (unix_timestamp(indexdate) - unix_timestamp(chartTime)) > 14400 ")

    println("filtered count: " + chartevents_filtered.count())

    val w = Window.partitionBy($"hadmID", $"label", $"itemID").orderBy($"chartTime".desc)
    val features = chartevents_filtered.withColumn("rn", row_number.over(w)).where($"rn" === 1).drop($"rn")

    println("filtered single items per type count: " + features.count())
    features.take(10).foreach(println)

    /** create a item_id to zero based id map */
    val feature_map = features.map(x => x.getInt(2)).rdd.distinct().sortBy(x => x).collect.zipWithIndex.toMap
    feature_map.take(10).foreach(println)
    val feature_length = feature_map.size
    println("Feature length: " + feature_length)

    val featuresDF= features
      .map(x => ((x.getInt(0), x.getInt(1)), (feature_map(x.getInt(2)), x.getDouble(4))))
      .rdd.groupByKey().map(x => LabeledPoint(x._1._2.toDouble, Vectors.sparse(feature_length, x._2.toArray)))
      .toDF("label","features").coalesce(1)

    featuresDF.take(5).foreach(println)

    featuresDF.write.mode(SaveMode.Overwrite).format("libsvm").save(saveDir+"/svmoutput")

//    /* Get the hadmid to label mapping */
//    val pat_labels = pat_features.keys.collect().toMap
//    val pat_featuremap = pat_features.sortBy(x => (x._1._1, x._2._1), ascending=true)
//                                     .groupByKey()
//      .map(x => LabeledPoint(x._1._2.toDouble, x._2.))

//    println("Feature set size: " + pat_features.count())

    chartevents.sparkContext.emptyRDD
  }
}



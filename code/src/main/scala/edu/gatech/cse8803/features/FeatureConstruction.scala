/**
  * @Author Hazel John on 4/1/18.
  */
package edu.gatech.cse8803.features

import java.sql.Timestamp

import edu.gatech.cse8803.model.ChartEvents
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.ml.feature.LabeledPoint

object FeatureConstruction {

  /* Define length of prediction window in milliseconds */
  val PREDICTION_WINDOW_MS = 4 * 60 * 60 * 1000
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
  def constructChartEventsFeatureTuple(indexdates: RDD[(Int, Timestamp, Int)], chartevents: RDD[ChartEvents]):
                                    RDD[FeatureTuple] = {

    /** Group index dates by icustay_id */
    val indexdates_grouped = indexdates.map(x => (x._1, (x._2, x._3)))

    /** Group chartevents by icustay id */
    val chartevents_grouped = chartevents.map(x => (x.hadmID, x))

    val chartevents_indexdates = chartevents_grouped
                                         .join(indexdates_grouped)

    /* Select latest chartevent value per type per icustayis */
    val pat_features = chartevents_indexdates
                        /* Remove chartevents falling in prediction window or later */
                        .filter(x => (x._2._2._1.getTime - x._2._1.chartTime.getTime) >= PREDICTION_WINDOW_MS)
                        /* map to ((label, hadmid, itemid), (timestamp, value)) */
                        .map(x => ((x._2._2._2, x._1, x._2._1.itemID), (x._2._1.chartTime.getTime, x._2._1.value)))
                        /* Reduce by key and select latest feature value per type */
                        .reduceByKey((v1, v2) => if (v1._1 > v2._1) v1 else v2 )
                        /* map to ((hadmid, label), (itemid, itemvalue)) records */
                        .map(x => ((x._1._1, x._1._2), (x._1._3, x._2._2))).cache()

//    /* Get the hadmid to label mapping */
//    val pat_labels = pat_features.keys.collect().toMap
//    val pat_featuremap = pat_features.sortBy(x => (x._1._1, x._2._1), ascending=true)
//                                     .groupByKey()
//      .map(x => LabeledPoint(x._1._2.toDouble, x._2.))

//    println("Feature set size: " + pat_features.count())

    chartevents.sparkContext.emptyRDD
  }
}



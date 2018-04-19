/**
  * @Author Hazel John on 3/31/18.
  */
package bdhproject.sepsis.windowing

import java.sql.Timestamp

import bdhproject.sepsis.model.{ChartEvents, ICUStay, Prescriptions, MicrobiologyEvents}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object TimeframeOperations {
  /* Specify milliseconds per hour to use to calculate time difference in hours */
  val MILLISPERHOUR = 3600000.0

  /** Calculate Index date as the time of the first suspicion of infection
    *
    */
  def calculateIndexDate(ss: SparkSession, icustays: RDD[ICUStay], prescriptions: RDD[Prescriptions],
              microbiologyEvents: RDD[MicrobiologyEvents]): RDD[(Int, Timestamp, Int)] = {

    /* Map hadm_ids to antibiotics dates */
    val pat_abxdates = prescriptions.map(x => (x.hadmID, x.startDate))
                                    .sortBy(x => (x._1, x._2.getTime), ascending=true).cache()

    /* Get earliest antibiotic prescription by admission ID */
    val earliest_prescription = pat_abxdates.reduceByKey((v1, v2) => if (v1.before(v2)) v1 else v2)

    /* Map hadm_ids to blood culture dates */
    val pat_bcdates = microbiologyEvents.map(x => (x.hadmID, x.chartDate))
                                        .sortBy(x => (x._1, x._2.getTime), ascending=true).cache().cache()

    /* Get earliest antibiotic prescription by admission ID */
    val earliest_bloodculture = pat_bcdates.reduceByKey((v1, v2) => if (v1.before(v2)) v1 else v2)

    /* Determine first of antibiotic or bloodculte and note if antibiotic "A" or blood culture "B" came first */
    val earliest_events = earliest_bloodculture.join(earliest_prescription)
                              .map(x => {
                                if (x._2._1.before(x._2._2) )
                                  (x._1, "B", x._2._1)
                                else
                                  (x._1, "A", x._2._2)})

    /** Retrieve hadm_ids with associated timestamp for patients where blood culture came first */
    val pat_first_bc = earliest_events.filter(x => x._2 == "B").map(x => (x._1, x._3))

    /** Retrieve hadm_ids with associated timestamp for patients where antibiotics came first */
    val pat_first_abx = earliest_events.filter(x => x._2 == "A").map(x => (x._1, x._3))

    /* Get patient index dates where we have blood culture followed by antibiotics within 24 hours as antibiotics time */
    val pat_bcfirst_abx = pat_abxdates.join(pat_first_bc)
                                      .filter(x => (x._2._1.getTime - x._2._2.getTime)/MILLISPERHOUR <= 24.0)
                                      .reduceByKey((v1, v2) => if (v1._2.before(v2._2)) v1 else v2)
                                      .map(x => (x._1, x._2._2))

    /* Get patient index dates where we have antibiotics followed by blood culture within 72 hours as antibiotics time */
    val pat_abxfirst_bc = pat_bcdates.join(pat_first_abx)
                                     .filter(x => (x._2._1.getTime - x._2._2.getTime)/MILLISPERHOUR <= 72.0)
                                     .reduceByKey((v1, v2) => if (v1._2.before(v2._2)) v1 else v2)
                                     .map(x => (x._1, x._2._2))

    /** Combine both patient sets */
    val pat_index_dates = pat_bcfirst_abx.union(pat_abxfirst_bc)

    /** Join with icustays to retrieve data on all icustays
      * Get the icustayid (not hadmid) and the index date if present, or icu outtime */
    val pat_allindexdates = icustays.map(x => (x.hadmID, x))
                               .leftOuterJoin(pat_index_dates)
                               .map(x => (x._1, x._2._2.getOrElse(x._2._1.outTime), if (x._2._2 == None) 0 else 1))
                               .distinct()
                               .cache

    println("Non-septic patients (control): " + pat_allindexdates.filter(x => x._3 == 0).count)
    println("Septic patients (case): " + pat_allindexdates.filter(x => x._3 == 1).count)
    println("patient index dates count: " + pat_allindexdates.count())

    pat_allindexdates
  }
}

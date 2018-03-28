/**
 * @author Hang Su <hangsu@gatech.edu>.
 */

package edu.gatech.cse8803.main

import java.lang.InterruptedException
import java.time.format.DateTimeFormatter
import java.time.LocalDate
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale

import edu.gatech.cse8803.clustering.Metrics
import edu.gatech.cse8803.features.FeatureConstruction
import edu.gatech.cse8803.ioutils.CSVUtils
import edu.gatech.cse8803.model.{Diagnostic, ICUStay, LabResult, Medication, Patient}
import edu.gatech.cse8803.phenotyping.T2dmPhenotype
import io.netty.util.internal.chmv8.ConcurrentHashMapV8.ObjectToInt
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.{Duration, Milliseconds, Seconds, StreamingContext}
import org.apache.spark.mllib.clustering.{GaussianMixture, KMeans, StreamingKMeans}
import org.apache.spark.mllib.linalg.{DenseMatrix, Matrices, Vector, Vectors}
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.Queue
import scala.util.Try
import scala.io.Source


object Main {

  /* Used for parsing numbers */
  val formatter = java.text.NumberFormat.getNumberInstance(java.util.Locale.US)

  val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def main(args: Array[String]) {
    import org.apache.log4j.Logger
    import org.apache.log4j.Level

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val sc = createContext
    val sqlContext = new SQLContext(sc)

    /** Retrieve static information about  antibiotics, sepsis icd9 codes and vitals itemids */
    val (antibiotics, sepsis_codes, vitals_itemids) = loadStaticRawData(sqlContext)

    /** Load patient data including icustay info, chart, prescription and diagnosis events */
    val (medication, labResult, diagnostic) = loadRddRawData(sqlContext, antibiotics, sepsis_codes, vitals_itemids)
//    val (candidateMedication, candidateLab, candidateDiagnostic) = loadLocalRawData
//
//    /** conduct phenotyping */
//    val phenotypeLabel = T2dmPhenotype.transform(medication, labResult, diagnostic)
//
//    /** feature construction with all features */
//    val featureTuples = sc.union(
//      FeatureConstruction.constructDiagnosticFeatureTuple(diagnostic),
//      FeatureConstruction.constructLabFeatureTuple(labResult),
//      FeatureConstruction.constructMedicationFeatureTuple(medication)
//    )
//
//    val rawFeatures = FeatureConstruction.construct(sc, featureTuples)
//
//    val (kMeansPurity, gaussianMixturePurity, streamKmeansPurity, nmfPurity) = testClustering(phenotypeLabel, rawFeatures)
//    println(f"[All feature] purity of kMeans is: $kMeansPurity%.5f")
//    println(f"[All feature] purity of GMM is: $gaussianMixturePurity%.5f")
//    println(f"[All feature] purity of StreamingKMeans is: $streamKmeansPurity%.5f")
//    println(f"[All feature] purity of NMF is: $nmfPurity%.5f")
//
//    /** feature construction with filtered features */
//    val filteredFeatureTuples = sc.union(
//      FeatureConstruction.constructDiagnosticFeatureTuple(diagnostic, candidateDiagnostic),
//      FeatureConstruction.constructLabFeatureTuple(labResult, candidateLab),
//      FeatureConstruction.constructMedicationFeatureTuple(medication, candidateMedication)
//    )
//
//    val filteredRawFeatures = FeatureConstruction.construct(sc, filteredFeatureTuples)
//
//    diagnostic.unpersist(false)
//    labResult.unpersist(false)
//    medication.unpersist(false)
//
//    val (kMeansPurity2, gaussianMixturePurity2, streamKmeansPurity2, nmfPurity2) = testClustering(phenotypeLabel, filteredRawFeatures)
//    println(f"[Filtered feature] purity of kMeans is: $kMeansPurity2%.5f")
//    println(f"[Filtered feature] purity of GMM is: $gaussianMixturePurity2%.5f")
//    println(f"[Filtered feature] purity of StreamingKMeans is: $streamKmeansPurity2%.5f")
//    println(f"[Filtered feature] purity of NMF is: $nmfPurity2%.5f")
    sc.stop 
  }

  /**
    * Retrieve information about antibiotics, sepsis icd9 codes and vital measurements itemids
    * @return
   */
  def loadStaticRawData(sqlContext: SQLContext): (Set[String], Set[String], Set[Int]) = {
    val antibiotics = Source.fromFile("data/antibiotics.txt").getLines().map(_.toLowerCase).toSet[String]
    val sepsis_codes = Source.fromFile("data/icd9_sepsis.txt").getLines().toSet[String]
    val vitals_defn = CSVUtils.loadCSVAsTable(sqlContext, "data/vitals_definitions.csv")
    val vitals_itemids = sqlContext.sql("SELECT itemid FROM vitals_definitions").map(s=> s(0).toString.toInt).collect().toSet
    (antibiotics, sepsis_codes, vitals_itemids)
  }

  /**
    *
    * @param s
    * @return double value or NaN in case of parse error
    */
  def parseDouble(s: String): Double = Try { val s2 = s.replaceAll(",", ""); s2.toDouble }.toOption.getOrElse(Double.NaN)

  def getYearDiff(end: String, start: String): Double = {
    (dateFormat.parse(start).getTime - dateFormat.parse(end).getTime)/365.2
  }
  /**
    *
    * @param sqlContext
    * @return tuple of medication, labresult and diagnostic RDDs
    */
  def loadRddRawData(sqlContext: SQLContext, antibiotics: Set[String], sepsis_codes: Set[String],
                     vitals_itemids: Set[Int]): (RDD[Medication], RDD[LabResult], RDD[Diagnostic]) = {

    /** First get a list of patients */
    val patients = CSVUtils.loadCSVAsTable(sqlContext, "data/PATIENTS.csv")
//    println("Total patients: " + patients.count)
//    patients.take(5).foreach(println)

    /** Retrieve the list of ICUSTAYS */
    val icustays_all = CSVUtils.loadCSVAsTable(sqlContext, "data/ICUSTAYS.csv")
//    println("Total icustays: " + icustays_all.count)
//    icustays_all.take(5).foreach(println)

//    sqlContext.udf.register("getYearDiff", getYearDiff _)

    /** Filter ICUSTAYS to retrieve patients >= 15yrs with metavision as the DBSOurce */
    val icustays_filtered = sqlContext.sql("SELECT ICUSTAY_ID, ICUSTAYS.SUBJECT_ID, HADM_ID, " +
                                  "to_date(ICUSTAYS.INTIME) as INTIME, to_date(ICUSTAYS.OUTTIME) as OUTTIME " +
                                  "FROM ICUSTAYS INNER JOIN PATIENTS " +
                                  "ON ICUSTAYS.SUBJECT_ID = PATIENTS.SUBJECT_ID " +
                                  "WHERE ICUSTAYS.DBSOURCE = 'metavision' " +
                                  "AND datediff(INTIME, to_date(PATIENTS.DOB))/365.2 >= 15.0")

//    println("Total icustays_filtered: " + icustays_filtered.count)
//    icustays_filtered.take(5).foreach(println)

    /** Convert in RDD */
    val icustays =  icustays_filtered.map(row => ICUStay(row.getString(0).toInt, row.getString(1).toInt,
                                                         row.getString(2).toInt, row.getDate(3), row.getDate(4))).cache()

//    println("icustays instances: " + icustays.count)
//    icustays.take(5).foreach(println)

    /** Retrieve the ICUSTAY IDs to filter chartevents */
    val icustay_ids = icustays_filtered.select("ICUSTAY_ID").distinct()
    icustay_ids.registerTempTable("ICUSTAY_IDS")

    val vitals = vitals_itemids.mkString("(", ",", ")")
//    println("vitals : " + vitals)

    /** Retrieve Chartevents */
    val chartevents_all = CSVUtils.loadCSVAsTable(sqlContext, "data/CHARTEVENTS.csv").coalesce(100)
    //println("Total chartevents : " + chartevents_all.count)
    chartevents_all.take(5).foreach(println)

    var chartevents = sqlContext.sql("SELECT CHARTEVENTS.ICUSTAY_ID, CHARTEVENTS.SUBJECT_ID, CHARTEVENTS.HADM_ID,  " +
                                     "CHARTEVENTS.ITEMID, CHARTEVENTS.CHARTTIME, CHARTEVENTS.VALUENUM " +
                                     "FROM CHARTEVENTS WHERE CHARTEVENTS.ITEMID IN " + vitals +
                                     "AND CHARTEVENTS.ERROR = 0 " +
                                     "AND VALUENUM IS NOT NULL ").cache()

//    "FROM CHARTEVENTS INNER JOIN ICUSTAY_IDS " +
//      "ON CHARTEVENTS.ICUSTAY_ID = ICUSTAY_IDS.ICUSTAY_ID " +

    chartevents.take(5).foreach(println)
    println("Total chartevents: " + chartevents.count())
//    /** load data using Spark SQL into three RDDs and return them
//      * Hint: You can utilize edu.gatech.cse8803.ioutils.CSVUtils and SQLContext.
//      *
//      * Notes:Refer to model/models.scala for the shape of Medication, LabResult, Diagnostic data type.
//      *       Be careful when you deal with String and numbers in String type.
//      *       Ignore lab results with missing (empty or NaN) values when these are read in.
//      *       For dates, use Date_Resulted for labResults and Order_Date for medication.
//      * */
//
//    /** Read medication data, convert medication name to lowercase to support phenotyping */
//    val medication: RDD[Medication] = CSVUtils.loadCSVAsTable(sqlContext, "data/medication_orders_INPUT.csv")
//                                              .map(s => Medication(s(1).toString,
//                                                                   dateFormat.parse(s(11).asInstanceOf[String]),
//                                                                   s(3).toString.toLowerCase))
//                                              .cache()
//    /** Read lab results data, converting lab name to lowercase to support phenotyping */
//    val lab =  CSVUtils.loadCSVAsTable(sqlContext, "data/lab_results_INPUT.csv")
//    val lab_filtered =  sqlContext.sql("SELECT Member_ID, Date_Resulted, Result_Name, Numeric_Result " +
//                                        "FROM lab_results_INPUT WHERE Numeric_Result != '' ")
//    val labResult: RDD[LabResult] =  lab_filtered.map(s => LabResult(s(0).toString, dateFormat.parse(s(1).asInstanceOf[String]),
//                                                                     s(2).toString.toLowerCase, parseDouble(s(3).toString)))
//                                                 .filter(s => !s.value.isNaN)
//                                                 .cache()
//
//    /** Read diagnostic data */
//    /** encounter_dx can have multiple codes for same encounter, so join creates multiple rows for an encounter **/
//    val encounter =  CSVUtils.loadCSVAsTable(sqlContext, "data/encounter_INPUT.csv")
//    val encounter_dx =  CSVUtils.loadCSVAsTable(sqlContext, "data/encounter_dx_INPUT.csv")
//    val encounter_coded = sqlContext.sql("SELECT Member_ID,Encounter_DateTime, code " +
//                                         "FROM encounter_INPUT LEFT OUTER JOIN encounter_dx_INPUT " +
//                                         "ON encounter_INPUT.Encounter_ID = encounter_dx_INPUT.Encounter_ID")
//
//    val diagnostic: RDD[Diagnostic] = encounter_coded.map(s => Diagnostic(s(0).toString,
//                                                                          dateFormat.parse(s(1).asInstanceOf[String]),
//                                                                          s(2).toString.trim))
//                                                     .cache()
    /** TODO: implement your own code here and remove existing placeholder code below */
    val medication: RDD[Medication] =  sqlContext.sparkContext.emptyRDD
    val labResult: RDD[LabResult] =  sqlContext.sparkContext.emptyRDD
    val diagnostic: RDD[Diagnostic] =  sqlContext.sparkContext.emptyRDD

    (medication, labResult, diagnostic)
  }

  def createContext(appName: String, masterUrl: String): SparkContext = {
    val conf = new SparkConf().setAppName(appName).setMaster(masterUrl)
    new SparkContext(conf)
  }

  def createContext(appName: String): SparkContext = createContext(appName, "local")

  def createContext: SparkContext = createContext("CSE 8803 Homework Two Application", "local")
}

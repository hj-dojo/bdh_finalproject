/**
 * @author Hang Su <hangsu@gatech.edu>.
 */

package edu.gatech.cse8803.main

import java.lang.InterruptedException
import java.text.SimpleDateFormat
import java.sql.Date
import java.io.File

import edu.gatech.cse8803.clustering.Metrics
import edu.gatech.cse8803.features.FeatureConstruction
import edu.gatech.cse8803.ioutils.CSVUtils
import edu.gatech.cse8803.ioutils.ParquetUtils
import edu.gatech.cse8803.model.{ChartEvents, ICUStay, Prescriptions, MicrobiologyEvents}
import edu.gatech.cse8803.phenotyping.T2dmPhenotype
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.{Duration, Milliseconds, Seconds, StreamingContext}
import org.apache.spark.mllib.clustering.{GaussianMixture, KMeans, StreamingKMeans}
import org.apache.spark.mllib.linalg.{DenseMatrix, Matrices, Vector, Vectors}
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.{SparkConf, SparkContext, SparkException}

import scala.collection.mutable.Queue
import scala.util.Try
import scala.io.Source


object Main {

  /* Used for parsing numbers */
  val formatter = java.text.NumberFormat.getNumberInstance(java.util.Locale.US)

  val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  val BLOOD_CULTURE_SPEC_ITEMID = 70012

  val usage = """
    Usage: spark-submit --master <masterurl> --class edu.gatech.cse8803.main.Main <target jar> [--reload] [--savedir dirpath]
  """

  def main(args: Array[String]) {
    import org.apache.log4j.Logger
    import org.apache.log4j.Level

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val ss = createSession("BDH Final Project", "local[*]")

    println("Input args: " + args.mkString(","))
    var reload = true
    var saveDir = "output"
    if (args.length > 0) {
      args.sliding(2, 2).toList.collect {
        case Array("--reload", argReload: String) => reload = (if (argReload.toInt == 1) true else false)
        case Array("--savedir", argSaveDir: String) => saveDir = argSaveDir
      }
    }

    /** Retrieve static information about  antibiotics, sepsis icd9 codes and vitals itemids */
    val (antibiotics, sepsis_codes, vitals_itemids) = loadStaticRawData(ss)

    /** Load patient data including icustay info, chart, prescription and microbiology events */
    val (icustays, chartevents, prescriptions, microbiologyevents) = if (!new File(saveDir).exists)
      loadRddRawData(ss, saveDir, antibiotics, sepsis_codes, vitals_itemids)
    else
      loadRddSavedData(ss, saveDir)

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
    ss.stop
  }

  /**
    * Retrieve information about antibiotics, sepsis icd9 codes and vital measurements itemids
    * @return
   */
  def loadStaticRawData(ss: SparkSession): (Set[String], Set[String], Set[Int]) = {
    import ss.implicits._
    val antibiotics = Source.fromFile("data/antibiotics.txt").getLines().map(_.toLowerCase).toSet[String]
    val sepsis_codes = Source.fromFile("data/icd9_sepsis.txt").getLines().toSet[String]
    val vitals_defn = CSVUtils.loadCSVAsTable(ss, "data/vitals_definitions.csv")
    val vitals_itemids = ss.sql("SELECT itemid FROM vitals_definitions").map(s => s.getInt(0)).collect().toSet
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
    * @param ss
    * @return tuple of ICUStay, ChartEvents, Prescriptions and MicrobiologyEvents after processing RAW files
    */
  def loadRddRawData(ss: SparkSession, saveDir:String, antibiotics: Set[String], sepsis_codes: Set[String],
                     vitals_itemids: Set[Int]): (RDD[ICUStay], RDD[ChartEvents],
                     RDD[Prescriptions], RDD[MicrobiologyEvents]) = {

    import ss.implicits._

    /** First get a list of patients */
    val patients = CSVUtils.loadCSVAsTable(ss, "data/PATIENTS.csv")
//    println("Total patients: " + patients.count)
//    patients.take(5).foreach(println)

    /** Retrieve the list of ICUSTAYS */
    val icustays_all = CSVUtils.loadCSVAsTable(ss, "data/ICUSTAYS.csv")
//    println("Total icustays: " + icustays_all.count)
//    icustays_all.take(5).foreach(println)

//    ss.udf.register("getYearDiff", getYearDiff _)

    /** Filter ICUSTAYS to retrieve patients >= 15yrs with metavision as the DBSOurce */
    val icustays_filtered = ss.sql("SELECT ICUSTAY_ID, ICUSTAYS.SUBJECT_ID, HADM_ID, " +
                                  "to_date(ICUSTAYS.INTIME) as INTIME, to_date(ICUSTAYS.OUTTIME) as OUTTIME " +
                                  "FROM ICUSTAYS INNER JOIN PATIENTS " +
                                  "ON ICUSTAYS.SUBJECT_ID = PATIENTS.SUBJECT_ID " +
                                  "WHERE ICUSTAYS.DBSOURCE = 'metavision' " +
                                  "AND datediff(INTIME, to_date(PATIENTS.DOB))/365.2 >= 15.0")

//    println("Total icustays_filtered: " + icustays_filtered.count)
//    icustays_filtered.take(5).foreach(println)

    /** Convert to RDD */
    val icustays =  icustays_filtered.rdd.map(row => ICUStay(row.getInt(0), row.getInt(1), row.getInt(2),
                              row.getDate(3), row.getDate(4)))
//    println("icustays instances: " + icustays.count)
//    icustays.take(5).foreach(println)

    /** Store to reduce processing time in subsequent runs */
    ParquetUtils.saveDataFrameAsParquet(ss, icustays.toDF(), saveDir+"/icustays")

    /** Retrieve the ICUSTAY IDs to filter prescriptions and chartevents */
    val icustay_ids = icustays_filtered.select("ICUSTAY_ID").distinct()
    icustay_ids.createOrReplaceTempView("ICUSTAY_IDS")
    val icustay_id_list = icustays_filtered.map(x => x.getInt(0)).collect().toSet
    val icustay_id_list_B = ss.sparkContext.broadcast(icustay_id_list)

    /** List of valid precription routes */
    val routes = List("IV", "PO","PO/NG","ORAL", "IV DRIP", "IV BOLUS")
    val antibiotics_B = ss.sparkContext.broadcast(antibiotics)

    /** Retrieve Prescriptions */
    val prescriptions_all = CSVUtils.loadCSVAsTable(ss, "data/PRESCRIPTIONS.csv")

//    println("Total prescriptions : " + prescriptions_all.count)
//    prescriptions_all.take(5).foreach(println)
//    prescriptions_all.printSchema()

    val prescriptions_filtered = prescriptions_all.as("p").join(icustay_ids.as("i"),
            col("p.ICUSTAY_ID") === col("i.ICUSTAY_ID"), "inner")
              .filter(($"DRUG_TYPE" === "MAIN" ) &&
                      ($"ROUTE" isin ("IV", "PO","PO/NG","ORAL", "IV DRIP", "IV BOLUS")) &&
                      (lower($"DRUG") isin(antibiotics_B.value.toList:_*)))
    println("Filtered  prescriptions : " + prescriptions_filtered.count)
    prescriptions_filtered.take(5).foreach(println)

    /** Convert to RDD */
    val prescriptions =  prescriptions_filtered.rdd.map(row =>
                    Prescriptions(row.getInt(1), row.getInt(2), row.getInt(3),
                      if (row.isNullAt(4)) new Date(0) else new Date(row.getTimestamp(4).getTime),
                      if (row.isNullAt(5)) new Date(0) else new Date(row.getTimestamp(5).getTime),
                      row.getString(7), row.getString(14),row.getString(15) ))

    println("prescriptions instances: " + prescriptions.count)
    prescriptions.take(5).foreach(println)

    /** Store to reduce processing time in subsequent runs */
    ParquetUtils.saveDataFrameAsParquet(ss, prescriptions.toDF(), saveDir+"/prescriptions")

    /** Retrieve the SUBJECT IDs to filter microbiologyevents */
    val subject_ids = icustays_filtered.select("SUBJECT_ID").distinct()
    subject_ids.createOrReplaceTempView("SUBJECT_IDS")

    /** Retrieve microbiologyevents */
    val microbiologyevents_all = CSVUtils.loadCSVAsTable(ss, "data/MICROBIOLOGYEVENTS.csv")

    println("Total microbiologyevents : " + microbiologyevents_all.count)
    microbiologyevents_all.take(5).foreach(println)
    microbiologyevents_all.printSchema()

    val microbiologyevents_filtered = microbiologyevents_all.as("m").join(subject_ids.as("s"),
      col("m.SUBJECT_ID") === col("s.SUBJECT_ID"), "inner")
      .filter(($"SPEC_ITEMID" === BLOOD_CULTURE_SPEC_ITEMID ))
    println("Filtered  microbiologyevents : " + microbiologyevents_filtered.count)
    microbiologyevents_filtered.take(5).foreach(println)
    microbiologyevents_filtered.printSchema()

    /** Convert to RDD, use charttime if present, else use chartdate */
    val microbiologyevents =  microbiologyevents_filtered.rdd.map(row => MicrobiologyEvents(
                    row.getInt(1), row.getInt(2),
                    if (row.isNullAt(4)) new Date(row.getTimestamp(3).getTime) else new Date(row.getTimestamp(4).getTime)))

    println("microbiology instances: " + microbiologyevents.count)
    microbiologyevents.take(5).foreach(println)

    /** Store to reduce processing time in subsequent runs */
    ParquetUtils.saveDataFrameAsParquet(ss, microbiologyevents.toDF(), saveDir+"/microbiologyevents")

    val vitals = vitals_itemids.mkString("(", ",", ")")
    val vitals_B = ss.sparkContext.broadcast(vitals_itemids)

    /** Retrieve Chartevents */
    val chartevents_all = CSVUtils.loadCSVAsTable(ss, "data/CHARTEVENTS.csv")

//    println("Total chartevents : " + chartevents_all.count)
//    chartevents_all.take(5).foreach(println)

    val chartevents_filtered = chartevents_all.as("c").join(icustay_ids.as("i"),
                                col("c.ICUSTAY_ID") === col("i.ICUSTAY_ID"), "inner")
                      .filter(($"ERROR" === 0 ) &&
                              ($"ITEMID" isin(vitals_B.value.toList:_*)) &&
                              ($"VALUENUM".isNotNull ))

    chartevents_filtered.take(5).foreach(println)
    println("Total chartevents: " + chartevents_filtered.count())

    /** Convert to RDD */
    val chartevents =  chartevents_filtered.rdd.map(row => ChartEvents(row.getInt(1), row.getInt(2), row.getInt(3),
                            row.getInt(4), new Date(row.getTimestamp(5).getTime), row.getDouble(9 )))
    println("chartevents instances: " + chartevents.count)
    chartevents.take(5).foreach(println)

    /** Store to reduce processing time in subsequent runs */
    ParquetUtils.saveDataFrameAsParquet(ss, chartevents.toDF(), saveDir+"/chartevents")

    (icustays, chartevents, prescriptions, microbiologyevents)
  }

  /**
    *
    * @param ss
    * @return tuple of ICUStay, ChartEvents, Prescriptions and MicrobiologyEvents after processing saved files
    */
  def loadRddSavedData(ss: SparkSession, saveDir:String): (RDD[ICUStay], RDD[ChartEvents],
                                          RDD[Prescriptions], RDD[MicrobiologyEvents]) = {
    import ss.implicits._

    val icustays = ParquetUtils.loadParquetAsDataFrame(ss, saveDir+"/icustays").as[ICUStay].rdd
    val chartevents = ParquetUtils.loadParquetAsDataFrame(ss, saveDir+"/chartevents").as[ChartEvents].rdd
    val prescriptions = ParquetUtils.loadParquetAsDataFrame(ss, saveDir+"/prescriptions").as[Prescriptions].rdd
    val microbiologyevents = ParquetUtils.loadParquetAsDataFrame(ss, saveDir+"/microbiologyevents").as[MicrobiologyEvents].rdd

    println("icustays count: " + icustays.count() + " chartevents count: " + chartevents.count() +
      " prescriptions count: " + prescriptions.count() + " microbiologyevents count: " + microbiologyevents.count())
    (icustays, chartevents, prescriptions, microbiologyevents)
  }

  def createSession(appName: String, masterUrl: String): SparkSession = {
    val sparkSession = SparkSession
      .builder.appName(appName)
      .master(masterUrl)
      .getOrCreate()
    sparkSession
  }

  def createSession(appName: String): SparkSession = createSession(appName, "local")

  def createSession: SparkSession = createSession("BDH Final Project", "local")
}
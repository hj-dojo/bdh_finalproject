/**
  * @Author Hazel John on 4/1/18.
  */
package bdhproject.sepsis.main

import java.text.SimpleDateFormat
import java.sql.Timestamp
import java.io.File

import bdhproject.sepsis.features.FeatureConstruction._
import bdhproject.sepsis.ioutils.CSVUtils
import bdhproject.sepsis.ioutils.ParquetUtils
import bdhproject.sepsis.windowing.TimeframeOperations
import bdhproject.sepsis.model.{ChartEvents, ICUStay, MicrobiologyEvents, Prescriptions}
import bdhproject.sepsis.classification.Modeling._
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext, SparkException}

import scala.util.Try
import scala.io.Source


object Main {

  /* Used for parsing numbers */
  val formatter = java.text.NumberFormat.getNumberInstance(java.util.Locale.US)

  val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  val BLOOD_CULTURE_SPEC_ITEMID = 70012

  val usage = """
    Usage: spark-submit --master <masterurl> --class bdhproject.sepsis3.main.Main
        <target jar> [--reload 0/1] [--savedir dirpath] [--predwindow timeinhours] [--obswindow timeinhours]
  """

  def main(args: Array[String]) {
    import org.apache.log4j.Logger
    import org.apache.log4j.Level

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    /** Utilize the master url from spark default configuration or spark-submit parameter if available
      * else use default
      */
    val masterURL = Try (new SparkContext(new SparkConf()).master).getOrElse("local[*]")
    println("masterURL: " + masterURL)

    var ss:SparkSession = createSession("BDH Final Project", masterURL)

    var reload = false
    var saveDir = "output"
    var predWindow = 8
    var obsWindow = 24
    if (args.length > 0) {
      args.sliding(2, 2).toList.collect {
        case Array("--reload", argReload: String) => reload = (if (argReload.toInt == 1) true else false)
        case Array("--savedir", argSaveDir: String) => saveDir = argSaveDir
        case Array("--predwindow", argPredWindow: String) => predWindow = argPredWindow.toInt
        case Array("--obswindow", argObsWindow: String) => obsWindow = argObsWindow.toInt
      }
    }

    println("Running with output Dir: " + saveDir + ", reload flag: " + reload +
            ", prediction window: " + predWindow + ", observation window: " + obsWindow)

    /** Retrieve static information about  antibiotics, sepsis icd9 codes and vitals itemids */
    val (antibiotics, sepsis_codes, vitals_itemids) = loadStaticRawData(ss)
    val saveDirFile = new File(saveDir)

    /** Load patient data including icustay info, chart, prescription and microbiology events */
    val (icustays, chartevents, prescriptions, microbiologyevents) = if (reload || !saveDirFile.exists)
      loadRddRawData(ss, saveDir, antibiotics, sepsis_codes, vitals_itemids)
    else
      loadRddSavedData(ss, saveDir)

    /** Extract Index dates */
    val pat_indexdates = TimeframeOperations.calculateIndexDate(ss, icustays, prescriptions, microbiologyevents)

    /** Retrieve the label and features vector dataframe from latest events */
    val featureDF = constructFeaturesWithLatestEvents(ss, saveDir, pat_indexdates, chartevents, icustays,
                                                      predWindow, obsWindow)

    /** Run logistic regression with param search on train/test split for validation */
    /* Comment out since Random Forest gives better results */
    //runLogisticRegressionwithValidation(featureDF)

    /** Run RandomForest with param search on train/test split for validation */
    runRandomForestwithValidation(featureDF)

    /** Run GradientBoostedTrees with param search on train/test split for validation */
    runGradientBoostedTreeswithValidation(featureDF)

    /** Run MultilayerPerceptron with param search on train/test split for validation */
    runMultiLayerPerceptronwithValidation(featureDF)

    /** Commenting out the rest as using average vitals over an observation window size doesn't give as good results
      * as using latest within observation window
      */
//    /** Retrieve the label and features vector dataframe from latest events */
//    val aggfeatureDF = constructFeaturesWithAggregateEvents(ss, saveDir, pat_indexdates, chartevents, icustays,
//                                                            predWindow, obsWindow)
//
//    /** Run logistic regression with param search on train/test split for validation */
//    runLogisticRegressionwithValidation(aggfeatureDF)
//
//    /** Run RandomForest with param search on train/test split for validation */
//    runRandomForestwithValidation(aggfeatureDF)
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

    /** Delete output path is it exists */
    FileUtils.deleteDirectory(new File(saveDir))

//    ss.udf.register("getYearDiff", getYearDiff _)

    /** First get a list of patients */
    val patients = CSVUtils.loadCSVAsTable(ss, "data/PATIENTS.csv")
//    println("Total patients: " + patients.count)
//    patients.take(5).foreach(println)

    /** Retrieve the list of ICUSTAYS */
    val icustays_all = CSVUtils.loadCSVAsTable(ss, "data/ICUSTAYS.csv")
//    println("Total icustays: " + icustays_all.count)
//    icustays_all.take(5).foreach(println)

    /** Filter ICUSTAYS to retrieve patients >= 15yrs with metavision as the DBSOurce */
    val icustays_filtered = ss.sql("SELECT ICUSTAY_ID, ICUSTAYS.SUBJECT_ID, HADM_ID, " +
                                  "to_timestamp(ICUSTAYS.INTIME) as INTIME, to_timestamp(ICUSTAYS.OUTTIME) as OUTTIME, " +
                                  "DOD, ROUND(datediff(INTIME, PATIENTS.DOB)/365.242, 2) as AGE, GENDER, EXPIRE_FLAG " +
                                  "FROM ICUSTAYS INNER JOIN PATIENTS " +
                                  "ON ICUSTAYS.SUBJECT_ID = PATIENTS.SUBJECT_ID " +
                                  "WHERE ICUSTAYS.DBSOURCE = 'metavision' " +
                                  "AND ROUND(datediff(INTIME, PATIENTS.DOB)/365.242, 2) >= 15.0 " +
                                  "AND ROUND(datediff(INTIME, PATIENTS.DOB)/365.424, 2) < 90.0")

//    println("Total icustays_filtered: " + icustays_filtered.count)
//    icustays_filtered.take(5).foreach(println)

    /** Convert to RDD */
    val icustays =  icustays_filtered.rdd.map(row => ICUStay(row.getInt(1), row.getInt(2), row.getInt(0),
                              row.getTimestamp(3), row.getTimestamp(4), row.getTimestamp(5),
                              row.getDecimal(6).doubleValue(), row.getString(7), row.getInt(8)))

//    println("icustays instances: " + icustays.count)
//    icustays.take(5).foreach(println)

    /** Store to reduce processing time in subsequent runs */
    ParquetUtils.saveDataFrameAsParquet(ss, icustays.toDF(), saveDir+"/icustays")

    /** Retrieve the ICUSTAY IDs to filter prescriptions and chartevents */
    val icustay_ids = icustays_filtered.select("ICUSTAY_ID").distinct()
    icustay_ids.createOrReplaceTempView("ICUSTAY_IDS")

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

//    println("Filtered  prescriptions : " + prescriptions_filtered.count)
//    prescriptions_filtered.take(5).foreach(println)

    /** Convert to RDD */
    val prescriptions =  prescriptions_filtered.rdd.map(row =>
                    Prescriptions(row.getInt(1), row.getInt(2), row.getInt(3),
                      if (row.isNullAt(4)) new Timestamp(0) else row.getTimestamp(4),
                      if (row.isNullAt(5)) new Timestamp(0) else row.getTimestamp(5),
                      row.getString(7), row.getString(14),row.getString(15) ))

//    println("prescriptions instances: " + prescriptions.count)
//    prescriptions.take(5).foreach(println)

    /** Store to reduce processing time in subsequent runs */
    ParquetUtils.saveDataFrameAsParquet(ss, prescriptions.toDF(), saveDir+"/prescriptions")

    /** Retrieve the SUBJECT IDs to filter microbiologyevents */
    val subject_ids = icustays_filtered.select("SUBJECT_ID").distinct()
    subject_ids.createOrReplaceTempView("SUBJECT_IDS")

    /** Retrieve microbiologyevents */
    val microbiologyevents_all = CSVUtils.loadCSVAsTable(ss, "data/MICROBIOLOGYEVENTS.csv")

//    println("Total microbiologyevents : " + microbiologyevents_all.count)
//    microbiologyevents_all.take(5).foreach(println)
//    microbiologyevents_all.printSchema()

    val microbiologyevents_filtered = microbiologyevents_all.as("m").join(subject_ids.as("s"),
      col("m.SUBJECT_ID") === col("s.SUBJECT_ID"), "inner")
      .filter(($"SPEC_ITEMID" === BLOOD_CULTURE_SPEC_ITEMID ))

//    println("Filtered  microbiologyevents : " + microbiologyevents_filtered.count)
//    microbiologyevents_filtered.take(5).foreach(println)
//    microbiologyevents_filtered.printSchema()

    /** Convert to RDD, use charttime if present, else use chartdate */
    val microbiologyevents =  microbiologyevents_filtered.rdd.map(row => MicrobiologyEvents(
                    row.getInt(1), row.getInt(2),
                    if (row.isNullAt(4)) row.getTimestamp(3) else row.getTimestamp(4)))

//    println("microbiology instances: " + microbiologyevents.count)
//    microbiologyevents.take(5).foreach(println)

    /** Store to reduce processing time in subsequent runs */
    ParquetUtils.saveDataFrameAsParquet(ss, microbiologyevents.toDF(), saveDir+"/microbiologyevents")

    val vitals = vitals_itemids.mkString("(", ",", ")")
//    println("vitals : " + vitals)

    /** Retrieve Chartevents */
    val chartevents_all = CSVUtils.loadCSVAsTable(ss, "data/CHARTEVENTS.csv")

//    println("Total chartevents : " + chartevents_all.count)
//    chartevents_all.take(5).foreach(println)

    val chartevents_filtered = ss.sql("SELECT HADM_ID, SUBJECT_ID, ICUSTAY_ID, ITEMID, CHARTTIME, VALUENUM " +
                             "FROM CHARTEVENTS WHERE CHARTEVENTS.ITEMID IN " + vitals +
                             "AND CHARTEVENTS.ERROR = 0 " +
                             "AND VALUENUM IS NOT NULL " +
                             "AND CHARTEVENTS.ICUSTAY_ID IN ( " +
                             "SELECT ICUSTAY_ID FROM ICUSTAY_IDS)")

//    chartevents_filtered.take(5).foreach(println)
//    println("Total chartevents: " + chartevents_filtered.count())

    /** Convert to RDD */
    val chartevents =  chartevents_filtered.rdd.map(row => ChartEvents(row.getInt(1), row.getInt(0), row.getInt(2),
                            row.getInt(3), row.getTimestamp(4), row.getDouble(5)))

//    println("chartevents instances: " + chartevents.count)
//    chartevents.take(5).foreach(println)

    /** Store to reduce processing time in subsequent runs */
    ParquetUtils.saveDataFrameAsParquet(ss, chartevents.toDF(), saveDir+"/chartevents")

    println("icustays count: " + icustays.count() + " chartevents count: " + chartevents.count() +
      " prescriptions count: " + prescriptions.count() + " microbiologyevents count: " + microbiologyevents.count())

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
//    println("icustays instances: " + icustays.count)
//    icustays.take(5).foreach(println)

    val chartevents = ParquetUtils.loadParquetAsDataFrame(ss, saveDir+"/chartevents").as[ChartEvents].rdd
//    println("chartevents instances: " + chartevents.count)
//    chartevents.take(5).foreach(println)

    val prescriptions = ParquetUtils.loadParquetAsDataFrame(ss, saveDir+"/prescriptions").as[Prescriptions].rdd
//    println("prescriptions instances: " + prescriptions.count)
//    prescriptions.take(5).foreach(println)

    val microbiologyevents = ParquetUtils.loadParquetAsDataFrame(ss, saveDir+"/microbiologyevents").as[MicrobiologyEvents].rdd
//    println("microbiologyevents instances: " + microbiologyevents.count)
//    microbiologyevents.take(5).foreach(println)

    println("icustays count: " + icustays.count() + " chartevents count: " + chartevents.count() +
      " prescriptions count: " + prescriptions.count() + " microbiologyevents count: " + microbiologyevents.count())

    (icustays, chartevents, prescriptions, microbiologyevents)
  }

  def createSession(appName: String, masterUrl: String): SparkSession = {

    val sparkSession = SparkSession.builder
                          .appName(appName)
                          .master(masterUrl)
                          .getOrCreate()
    sparkSession
  }

  def createSession(appName: String): SparkSession = createSession(appName, "local")

  def createSession: SparkSession = createSession("BDH Final Project", "local")
}

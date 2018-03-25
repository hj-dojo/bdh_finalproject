/**
  * @author Hang Su <hangsu@gatech.edu>,
  * @author Sungtae An <stan84@gatech.edu>,
  */

package edu.gatech.cse8803.phenotyping

import edu.gatech.cse8803.model.{Diagnostic, LabResult, Medication}
import org.apache.spark.rdd.RDD

object T2dmPhenotype {
  
  // criteria codes given
  val T1DM_DX = Set("250.01", "250.03", "250.11", "250.13", "250.21", "250.23", "250.31", "250.33", "250.41", "250.43",
      "250.51", "250.53", "250.61", "250.63", "250.71", "250.73", "250.81", "250.83", "250.91", "250.93")

  val T2DM_DX = Set("250.3", "250.32", "250.2", "250.22", "250.9", "250.92", "250.8", "250.82", "250.7", "250.72", "250.6",
      "250.62", "250.5", "250.52", "250.4", "250.42", "250.00", "250.02")

  val T1DM_MED = Set("lantus", "insulin glargine", "insulin aspart", "insulin detemir", "insulin lente", "insulin nph", "insulin reg", "insulin,ultralente")

  val T2DM_MED = Set("chlorpropamide", "diabinese", "diabanase", "diabinase", "glipizide", "glucotrol", "glucotrol xl",
      "glucatrol ", "glyburide", "micronase", "glynase", "diabetamide", "diabeta", "glimepiride", "amaryl",
      "repaglinide", "prandin", "nateglinide", "metformin", "rosiglitazone", "pioglitazone", "acarbose",
      "miglitol", "sitagliptin", "exenatide", "tolazamide", "acetohexamide", "troglitazone", "tolbutamide",
      "avandia", "actos", "actos", "glipizide")

  val DM_RELATED_DX = Set("790.21", "790.22", "790.2", "790.29", "648.81", "648.82", "648.83", "648.84", "648", "648.01",
     "648.02", "648.03", "648.04", "791.5", "277.7", "V77.1", "256.4")

  val GLUCOSE_LABS = Set("hba1c", "hemoglobin a1c", "fasting glucose", "fasting blood glucose", "fasting plasma glucose",
      "glucose", "glucose, serum")

  /**
    *
    * @param x tuple with lab name (in lowercase) and value
    * @return true if abnormal values for glucose measure else false
    */
  def isAbnormalLab(x: (String, Double)) : Boolean = {
    val isAbnormal = x._1 match {
      case "hba1c"  => x._2 >= 6.0
      case "hemoglobin a1c"  => x._2 >= 6.0
      case "fasting glucose"  => x._2 >= 110
      case "fasting blood glucose"  => x._2 >= 110
      case "fasting plasma glucose"  => x._2 >= 110
      case "glucose" => x._2 > 110
      case "glucose, serum"  => x._2 > 110
//      case "glucose 3hr" => x._2 > 110
//      case "glucose 2hr post dose" => x._2 > 110
        /* could have used this if all glucose tests were treated consistently */
//      case lab if lab.contains("glucose") => x._2 >= 110
      case _  => false  // the default, catch-all
    }
    return isAbnormal
  }

  /**
    * Transform given data set to a RDD of patients and corresponding phenotype
    * @param medication medication RDD
    * @param labResult lab result RDD
    * @param diagnostic diagnostic code RDD
    * @return tuple in the format of (patient-ID, label). label = 1 if the patient is case, label = 2 if control, 3 otherwise
    */
  def transform(medication: RDD[Medication], labResult: RDD[LabResult], diagnostic: RDD[Diagnostic]): RDD[(String, Int)] = {
    /**
      * Hard code the medication, lab, icd code etc. for phenotypes like example code below.
      * When testing your code, we expect your function to have no side effect,
      * i.e. do NOT read from file or write file
      *
      * You don't need to follow the example placeholder code below exactly, but do have the same return type.
      *
      * Hint: Consider case sensitivity when doing string comparisons.
      */

    val sc = medication.sparkContext

    /* Convert T1 and T2 diagnosis and medication criteria codes to broadcast variables */
    val T1DM_DX_B = sc.broadcast(T1DM_DX)
    val T2DM_DX_B = sc.broadcast(T2DM_DX)
    val T1DM_MED_B = sc.broadcast(T1DM_MED)
    val T2DM_MED_B = sc.broadcast(T2DM_MED)

    /* Map patient id to diagnosis code */
    val pat_diagRDD = diagnostic.map(diag => (diag.patientID, diag.code))

    /* Extract patients with Type I diagnosis */
    val pat_t1diag = pat_diagRDD.filter{ case (k,v) => T1DM_DX_B.value.contains(v)}
                                        .keys.distinct.collect().toSet
//    println("Num patients with T1 diag: " +  pat_t1diag.size)

    /* Extract patients without Type I diagnosis but have type II */
    val pat_t2diag_not1diag = pat_diagRDD.filter { case (id, code) =>
                                                      if (T2DM_DX_B.value.contains(code) &&
                                                          !pat_t1diag.contains(id))
                                                        true
                                                      else
                                                        false
                                                }.keys.distinct.collect().toSet
//    println("Num patient with T2 diag but no T1 diag: " + pat_t2diag_not1diag.size)

    /* Map patient id to medication and order date */
    val pat_medRDD = medication.map(med => (med.patientID, (med.medicine, med.date)))

    /* get list of patients with type 1 medication */
    val pat_t1med = pat_medRDD.filter{ case (k,v) => T1DM_MED_B.value.contains(v._1)}.cache()

    /* Get CASE patients with T2 diag, no T1 diag and dosn't have order for TYPE 1 meds */
    val case_set1 = (pat_t2diag_not1diag -- pat_t1med.keys.distinct.collect().toSet)
//    println("Num patient with T2 diag, no T1 diag and no T1 med: " + case_set1.size)

    /* Get CASE patients with T2 diag, no T1 diag and have order for TYPE 1 meds */
    val pat_t2diag_not1diag_t1med = (pat_t2diag_not1diag -- case_set1)
//    println("Num patient with T2 diag, no T1 diag, with T1 med: " + pat_t2diag_not1diag_t1med.size)

    /* get list of patients with type 2 medication */
    val pat_t2med = pat_medRDD.filter{ case (k,v) => T2DM_MED_B.value.contains(v._1)}.cache()

    /* Get CASE patients with T2 diag, no T1 diag, T1 meds, but no T2 meds */
    val case_set2 = (pat_t2diag_not1diag_t1med -- pat_t2med.keys.distinct.collect().toSet)
//    println("Num patient with type 2, no T1 diag, T1 med, no T2 med: " + case_set2.size)

    /* Get patients with T2 diag, no T1 diag, T1 meds and T2 meds */
    val pat_t2diag_not1diag_t1t2med = (pat_t2diag_not1diag_t1med -- case_set2)
//    println("Num patient with type 2, no T1 diag, T1 med, T2 med: " + pat_t2diag_not1diag_t1t2med.size)

    /* Get patient's earliest T1 med order and join with patient's earliest T2 med order */
    val pat_t1t2dates = pat_t1med.reduceByKey((v1, v2) => if (v1._2.before(v2._2)) v1 else v2)
                                 .join(pat_t2med.reduceByKey((v1, v2) => if (v1._2.before(v2._2)) v1 else v2))

    /* Get patients with T2 diag, no T1 diag, T1 meds and T2 meds,
     * with T2 med order date before T1 med order date */
    val case_set3 = pat_t1t2dates.filter{case (k,v) => if (pat_t2diag_not1diag_t1t2med.contains(k) &&
                                                            v._2._2.before(v._1._2))
                                                          true
                                                       else
                                                          false }.keys.collect().toSet
//    println("Num patient with type 2, no T1 diag, T2 med date < T1 med date: " + case_set3.size)

    /** Combine CASE Patients */
    val all_casepatients = case_set1 ++ case_set2 ++ case_set3
    //val all_casepatients = sc.union(case_set1, case_set2, case_set3)
    /** Create CASE Patients */
    val casePatients = sc.parallelize(all_casepatients.toSeq.map(v => (v, 1)))

    /* Convert glucose labs and diabetis related diagnosis criteria codes to broadcast variables */
    val GLUCOSE_LABS_B = sc.broadcast(GLUCOSE_LABS)
    val DM_RELATED_DX_B = sc.broadcast(DM_RELATED_DX)

    /* Map patient id to lab name and lab results */
    val pat_labRDD = labResult.map(lab => (lab.patientID, (lab.testName, lab.value)))

    /* Retrieve all lab results related to glucose */
    //val pat_glucoselabs = pat_labRDD.filter{ case (k,v) => GLUCOSE_LABS_B.value.contains(v._1)}
    val pat_glucoselabs = pat_labRDD.filter{ case (k,v) => v._1.contains("glucose")}
                                    .keys.distinct().collect().toSet
//    println("Num patients with any glucose related lab: " + pat_glucoselabs.size)

    /* Retrieve patients with no abnormal results */
    val pat_gluc_abnormallabs = pat_labRDD.filter{ case (k,v) => pat_glucoselabs.contains(k) &&
                                                           isAbnormalLab(v)}.keys.distinct().collect().toSet

    /* Retrieve patients that done glucose tests but have with no abnormal labs(glucose + h1bac) */
    val pat_gluc_noabnormallabs = pat_glucoselabs -- pat_gluc_abnormallabs
//    println("Num patients with no abnormal glucose related labs: " + pat_gluc_noabnormallabs.size)

    /* Retrieve  patients with  diabetes diagnosis */
    val pat_diabetesdx = pat_diagRDD.filter{ case (k,v) => DM_RELATED_DX_B.value.contains(v) ||
                                                           v.startsWith("250")}
                                    .keys.distinct.collect().toSet

    /* Retrieve patients with glucose labs but no abnormal results, and no diabetes diagnosis */
    val all_controlpatients = pat_gluc_noabnormallabs.filter{case k => !pat_diabetesdx.contains(k)}
//    println("Num patients with glucose labs, no abnormal labs, no diabetes diag: " + all_controlpatients.size)

    /** Find CONTROL Patients */
    val controlPatients = sc.parallelize(all_controlpatients.toSeq.map(v => (v, 2)))

    /* Find all unique patients */
    val all_otherpatients = pat_diagRDD.keys.distinct().collect().toSet --
                            (all_controlpatients.union(all_casepatients))

    /** Find OTHER Patients */
    val others = sc.parallelize(all_otherpatients.toSeq.map(v => (v, 3)))
//    println("total patients: " + pat_diagRDD.keys.distinct().count + " case count: " + casePatients.keys.count() + " control count: " + controlPatients.keys.count() +
//            " other count: " + others.keys.count())

    /** Once you find patients for each group, make them as a single RDD[(String, Int)] */
    val phenotypeLabel = sc.parallelize(sc.union(casePatients, controlPatients, others).collect(), 100)

    /* Remove all uneeded cached data from memory */
    T1DM_DX_B.unpersist(false)
    T2DM_DX_B.unpersist(false)
    T1DM_MED_B.unpersist(false)
    T2DM_MED_B.unpersist(false)
    GLUCOSE_LABS_B.unpersist(false)
    DM_RELATED_DX_B.unpersist(false)
    pat_t1med.unpersist(false)
    pat_t2med.unpersist(false)

    /** Return */
    phenotypeLabel
  }

  /**
    * Transform given data set to a RDD of patients and corresponding phenotype
    * This version uses RDD union and subtract instead of set operations and is very inefficient
    * @param medication medication RDD
    * @param labResult lab result RDD
    * @param diagnostic diagnostic code RDD
    * @return tuple in the format of (patient-ID, label). label = 1 if the patient is case, label = 2 if control, 3 otherwise
    */
  def transform2(medication: RDD[Medication], labResult: RDD[LabResult], diagnostic: RDD[Diagnostic]): RDD[(String, Int)] = {
    /**
      * Hard code the medication, lab, icd code etc. for phenotypes like example code below.
      * When testing your code, we expect your function to have no side effect,
      * i.e. do NOT read from file or write file
      *
      * You don't need to follow the example placeholder code below exactly, but do have the same return type.
      *
      * Hint: Consider case sensitivity when doing string comparisons.
      */

    val sc = medication.sparkContext

    /* Convert T1 and T2 diagnosis and medication criteria codes to broadcast variables */
    val T1DM_DX_B = sc.broadcast(T1DM_DX)
    val T2DM_DX_B = sc.broadcast(T2DM_DX)
    val T1DM_MED_B = sc.broadcast(T1DM_MED)
    val T2DM_MED_B = sc.broadcast(T2DM_MED)

    /* Map patient id to diagnosis code */
    val pat_diagRDD = diagnostic.map(diag => (diag.patientID, diag.code)).cache()

    /* Extract patients with Type I diagnosis */
    val pat_t1diag = pat_diagRDD.filter{ case (id, code) => T1DM_DX_B.value.contains(code)}
                                .keys.distinct
    println("Num patients with T1 diag: " +  pat_t1diag.count())

    /* Extract patients with type II */
    val pat_t2diag = pat_diagRDD.filter { case (id, code) => T2DM_DX_B.value.contains(code)}
                                .keys.distinct

    val pat_t2diag_not1diag = pat_t2diag.subtract(pat_t1diag)
    println("Num patient with T2 diag but no T1 diag: " + pat_t2diag_not1diag.count())

    /* Map patient id to medication and order date */
    val pat_medRDD = medication.map(med => (med.patientID, (med.medicine, med.date)))

    /* get list of patients with type 1 medication */
    val pat_t1med = pat_medRDD.filter{ case (k,v) => T1DM_MED_B.value.contains(v._1)}.cache()

    /* Get CASE patients with T2 diag, no T1 diag and dosn't have order for TYPE 1 meds */
    val case_set1 = pat_t2diag_not1diag.subtract(pat_t1med.keys)
    println("Num patient with T2 diag, no T1 diag and no T1 med: " + case_set1.count())

    /* Get CASE patients with T2 diag, no T1 diag and have order for TYPE 1 meds */
    val pat_t2diag_not1diag_t1med = pat_t2diag_not1diag.subtract(case_set1)
    println("Num patient with T2 diag, no T1 diag, with T1 med: " + pat_t2diag_not1diag_t1med.count())

    /* get list of patients with type 2 medication */
    val pat_t2med = pat_medRDD.filter{ case (k,v) => T2DM_MED_B.value.contains(v._1)}.cache()

    /* Get CASE patients with T2 diag, no T1 diag, T1 meds, but no T2 meds */
    val case_set2 = pat_t2diag_not1diag_t1med.subtract(pat_t2med.keys)
    println("Num patient with type 2, no T1 diag, T1 med, no T2 med: " + case_set2.count())

    /* Get patients with T2 diag, no T1 diag, T1 meds and T2 meds */
    val pat_t2diag_not1diag_t1t2med = pat_t2diag_not1diag_t1med.subtract(case_set2)
    println("Num patient with type 2, no T1 diag, T1 med, T2 med: " + pat_t2diag_not1diag_t1t2med.count())

    /* Get patient's earliest T1 med order and join with patient's earliest T2 med order */
    val pat_t1t2dates = pat_t1med.reduceByKey((v1, v2) => if (v1._2.before(v2._2)) v1 else v2)
                                 .join(pat_t2med.reduceByKey((v1, v2) => if (v1._2.before(v2._2)) v1 else v2))

    /* Get patients whose T2 medication precedes T1 medication */
    val pat_t2medaftert1med = pat_t1t2dates.filter{case (k,v) => v._2._2.after(v._1._2)}.keys.distinct()

    /* Get patients with T2 diag, no T1 diag, T1 meds and T2 meds,
     * with T2 med order date before T1 med order date */
    val case_set3 = pat_t2diag_not1diag_t1t2med.subtract(pat_t2medaftert1med)
    println("Num patient with type 2, no T1 diag, T2 med date < T1 med date: " + case_set3.count())

    /** Create CASE Patients */
    val allcasepatients = sc.union(case_set1, case_set2, case_set3)
    val casePatients = allcasepatients.map(v => (v, 1))

    /* Convert diabetis related diagnosis criteria codes to broadcast variables */
    val DM_RELATED_DX_B = sc.broadcast(DM_RELATED_DX)

    /* Map patient id to lab name and lab results */
    val pat_labRDD = labResult.map(lab => (lab.patientID, (lab.testName, lab.value)))

    /* Retrieve all lab results related to glucose */
    val pat_glucoselabs = pat_labRDD.filter{ case (k,v) => v._1.contains("glucose")}
                                    .keys.distinct()
    println("Num patients with any glucose related lab: " + pat_glucoselabs.count())

    /* Retrieve patients with abnormal results */
    val pat_abnormallabs = pat_labRDD.filter{ case (k,v) => isAbnormalLab(v)}.keys.distinct()

    /* Retrieve patients that done glucose tests but have with no abnormal labs(glucose + h1bac) */
    val pat_gluc_noabnormallabs = pat_glucoselabs.subtract(pat_abnormallabs)
    println("Num patients with glucose labs, but no abnormal labs: " + pat_gluc_noabnormallabs.count())

    /* Retrieve  patients with  diabetes diagnosis */
    val pat_diabetesdx = pat_diagRDD.filter{ case (k,v) => DM_RELATED_DX_B.value.contains(v) ||
                                                           v.startsWith("250")}
                                    .keys.distinct()

    /* Retrieve patients with glucose labs but no abnormal results, and no diabetes diagnosis */
    val all_controlpatients = pat_gluc_noabnormallabs.subtract(pat_diabetesdx)
    println("Num patients with glucose labs, no abnormal labs, no diabetes diag: " + all_controlpatients.count())

    /** Find CONTROL Patients */
    val controlPatients = all_controlpatients.map(v => (v, 2))

    /* Find all unique patients */
    val all_otherpatients = pat_diagRDD.keys.distinct().subtract(all_controlpatients.union(allcasepatients))

    /** Find OTHER Patients */
    val others = all_otherpatients.map(v => (v, 3))
    println("total patients: " + pat_diagRDD.keys.distinct().count + " case count: " + casePatients.keys.count() + " control count: " + controlPatients.keys.count() +
                " other count: " + others.keys.count())

    /** Once you find patients for each group, make them as a single RDD[(String, Int)] */
    val phenotypeLabel = sc.union(casePatients, controlPatients, others)

    /** Return */
    phenotypeLabel
  }
}
